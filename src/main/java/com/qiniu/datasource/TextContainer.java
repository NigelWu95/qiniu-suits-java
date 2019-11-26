package com.qiniu.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.*;
import com.qiniu.util.*;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

public abstract class TextContainer<E, W, T> extends DatasourceActor implements IDataSource<IReader<E>, IResultOutput<W>, T> {

    protected String path;
    protected String parse;
    protected String separator;
    protected String addKeyPrefix;
    protected String rmKeyPrefix;
    protected Map<String, Map<String, String>> linesMap;
    protected ILineProcess<T> processor; // 定义的资源处理器
    protected ConcurrentMap<String, IResultOutput<W>> saverMap = new ConcurrentHashMap<>(threads);
    protected ConcurrentMap<String, ILineProcess<T>> processorMap = new ConcurrentHashMap<>(threads);

    public TextContainer(String path, String parse, String separator, String addKeyPrefix, String rmKeyPrefix,
                         Map<String, Map<String, String>> linesMap, Map<String, String> indexMap, List<String> fields,
                         int unitLen, int threads) throws IOException {
        super(unitLen, threads);
        this.path = path;
        this.parse = parse;
        this.separator = separator;
        this.addKeyPrefix = addKeyPrefix;
        this.rmKeyPrefix = rmKeyPrefix;
        this.linesMap = linesMap == null ? new HashMap<>() : linesMap;
        setIndexMapWithDefault(indexMap);
        if (fields != null && fields.size() > 0) this.fields = fields;
        else this.fields = ConvertingUtils.getOrderedFields(this.indexMap, null);
        // default save parameters，默认全记录保存
        setSaveOptions(true, "result", "tab", "\t", null);
    }

    private void setIndexMapWithDefault(Map<String, String> indexMap) throws IOException {
        if (indexMap == null || indexMap.size() == 0) {
            if (this.indexMap == null) this.indexMap = new HashMap<>();
            for (String fileInfoField : ConvertingUtils.defaultFileFields) {
                this.indexMap.put(fileInfoField, fileInfoField);
            }
        } else {
            for (String s : indexMap.keySet()) {
                if (s == null || "".equals(s)) throw new IOException("the index can not be empty in " + indexMap);
            }
            this.indexMap = indexMap;
        }
    }

    public void setProcessor(ILineProcess<T> processor) {
        this.processor = processor;
    }

    protected abstract ITypeConvert<E, T> getNewConverter() throws IOException;

    protected abstract ITypeConvert<T, String> getNewStringConverter() throws IOException;

    public void export(IReader<E> reader, IResultOutput<W> saver, ILineProcess<T> processor) throws Exception {
        ITypeConvert<E, T> converter = getNewConverter();
        ITypeConvert<T, String> stringConverter = null;
        if (saveTotal) {
            stringConverter = getNewStringConverter();
            saver.preAddWriter("failed");
        }
        String lastLine = reader.lastLine();
        List<E> srcList = null;
        List<T> convertedList;
        List<String> writeList;
        int retry;
        while (lastLine != null) {
            if (LocalDateTime.now(DatetimeUtils.clock_Default).isAfter(pauseDateTime)) {
                synchronized (object) {
                    object.wait();
                }
            }
            retry = retryTimes + 1;
            while (retry > 0) {
                try {
                    srcList = reader.readLines();
                    retry = 0;
                } catch (IOException e) {
                    retry--;
                    if (retry == 0) throw e;
                }
            }
            convertedList = converter.convertToVList(srcList);
            if (converter.errorSize() > 0) saver.writeError(converter.errorLines(), false);
            if (stringConverter != null) {
                writeList = stringConverter.convertToVList(convertedList);
                if (writeList.size() > 0) saver.writeSuccess(String.join("\n", writeList), false);
                if (stringConverter.errorSize() > 0)
                    saver.writeToKey("failed", stringConverter.errorLines(), false);
            }
            // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
            try {
                if (processor != null) processor.processLine(convertedList);
            } catch (QiniuException e) {
                // 这里其实逻辑上没有做重试次数的限制，因为返回的 retry 始终大于等于 -1，所以不是必须抛出的异常则会跳过，process 本身会
                // 保存失败的记录，除非是 process 出现 599 状态码才会抛出异常
                if (HttpRespUtils.checkException(e, 2) < -1) throw e;
                if (e.response != null) e.response.close();
            }
            try { FileUtils.createIfNotExists(procedureLogFile); } catch (IOException ignored) {}
            procedureLogger.info(recorder.put(reader.getName(), lastLine));
            lastLine = reader.lastLine();
        }
    }

    protected abstract IResultOutput<W> getNewResultSaver(String order) throws IOException;

    void reading(IReader<E> reader) {
        int order = UniOrderUtils.getOrder();
        String orderStr = String.valueOf(order);
        ILineProcess<T> lineProcessor = null;
        IResultOutput<W> saver = null;
        try {
            saver = getNewResultSaver(orderStr);
            saverMap.put(orderStr, saver);
            if (processor != null) {
                lineProcessor = processor.clone();
                processorMap.put(orderStr, lineProcessor);
            }
            export(reader, saver, lineProcessor);
            recorder.remove(reader.getName());
        }  catch (QiniuException e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}, {}", reader.getName(), recorder.getString(reader.getName()), e.error(), e);
            if (e.response != null) e.response.close();
        } catch (Throwable e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}", reader.getName(), recorder.getString(reader.getName()), e);
        } finally {
            try { FileUtils.createIfNotExists(infoLogFile); } catch (IOException ignored) {}
            infoLogger.info("{}\t{}\t{}", orderStr, reader.getName(), reader.count());
            if (saver != null) {
                saver.closeWriters();
                saver = null; // let gc work
            }
            saverMap.remove(orderStr);
            if (lineProcessor != null) {
                lineProcessor.closeResource();
                lineProcessor = null;
            }
            UniOrderUtils.returnOrder(order);
            reader.close();
        }
    }

    protected abstract IReader<E> getReader(File source, String start, int unitLen) throws IOException;

    private List<IReader<E>> getFileReaders(String path) throws IOException {
        List<IReader<E>> fileReaders = new ArrayList<>();
        if (linesMap != null && linesMap.size() > 0) {
            boolean pathIsValid = true;
            try { path = FileUtils.convertToRealPath(path); } catch (IOException ignored) { pathIsValid = false; }
            String type;
            File file;
            Map<String, String> map;
            String start;
            for (String filename : linesMap.keySet()) {
                if (pathIsValid) {
                    file = new File(path, filename);
                    if (!file.exists()) file = new File(filename);
                } else {
                    file = new File(filename);
                }
                if (!file.exists()) throw new IOException("the filename not exists: " + filename);
                if (file.isDirectory()) {
                    throw new IOException("the filename defined in lines map can not be directory: " + filename);
                } else {
                    type = FileUtils.contentType(file);
                    if (type.startsWith("text") || type.equals("application/octet-stream")) {
                        map = linesMap.get(filename);
                        start = map == null ? null : map.get("start");
                        fileReaders.add(getReader(file, start, unitLen));
                    } else {
                        throw new IOException("please provide the \'text\' file. The current path you gave is: " + path);
                    }
                }
            }
        } else {
            path = FileUtils.convertToRealPath(path);
            File sourceFile = new File(path);
            if (sourceFile.isDirectory()) {
                List<File> files = FileUtils.getFiles(sourceFile, true);
                for (File file : files) {
                    if (file.getPath().contains(FileUtils.pathSeparator + ".")) continue;
                    fileReaders.add(getReader(file, null, unitLen));
                }
            } else {
                String type = FileUtils.contentType(sourceFile);
                if (type.startsWith("text") || type.equals("application/octet-stream")) {
                    fileReaders.add(getReader(sourceFile, null, unitLen));
                } else {
                    throw new IOException("please provide the \'text\' file. The current path you gave is: " + path);
                }
            }
        }
        if (fileReaders.size() == 0) throw new IOException("please provide the \'text\' file in the directory. " +
                "The current path you gave is: " + path);
        return fileReaders;
    }

    public void export() throws Exception {
        String info = processor == null ?
                String.join(" ", "read lines from path:", path) :
                String.join(" ", "read lines from path:", path, "and", processor.getProcessName());
        rootLogger.info("{} running...", info);
        rootLogger.info("order\tpath\tquantity");
        showdownHook();
        List<IReader<E>> fileReaders = getFileReaders(path);
        int filesCount = fileReaders.size();
        int runningThreads = filesCount < threads ? filesCount : threads;
        executorPool = Executors.newFixedThreadPool(runningThreads);
        try {
            String start = null;
            for (IReader<E> fileReader : fileReaders) {
                recorder.put(fileReader.getName(), start);
                executorPool.execute(() -> reading(fileReader));
            }
            executorPool.shutdown();
            while (!executorPool.isTerminated()) {
                sleep(2000);
            }
            rootLogger.info("{} finished.", info);
            endAction();
        } catch (Throwable e) {
            executorPool.shutdownNow();
            rootLogger.error("export failed", e);
            endAction();
            System.exit(-1);
        }
    }
}
