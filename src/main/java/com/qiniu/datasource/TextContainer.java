package com.qiniu.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.*;
import com.qiniu.util.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class TextContainer<T> extends DatasourceActor implements IDataSource<ITextReader, IResultOutput, T> {

    protected String path;
    protected String parse;
    protected String separator;
    protected String addKeyPrefix;
    protected String rmKeyPrefix;
    protected List<String> antiPrefixes;
    protected boolean hasAntiPrefixes = false;
    protected Map<String, Map<String, String>> urisMap;
    protected List<String> uris;
    protected ILineProcess<T> processor; // 定义的资源处理器

    public TextContainer(String path, String parse, String separator, Map<String, Map<String, String>> urisMap,
                         List<String> antiPrefixes, String addKeyPrefix, String rmKeyPrefix, Map<String, String> indexMap,
                         List<String> fields, int unitLen, int threads) throws IOException {
        super(unitLen, threads);
        this.path = path;
        this.parse = parse;
        this.separator = separator;
        this.addKeyPrefix = addKeyPrefix;
        this.rmKeyPrefix = rmKeyPrefix;
        setAntiPrefixes(antiPrefixes);
        setUrisAndMap(urisMap);
        setIndexMapWithDefault(indexMap);
        if (fields != null && fields.size() > 0) this.fields = fields;
        else this.fields = ConvertingUtils.getOrderedFields(this.indexMap, null);
        // default save parameters，默认全记录保存
        setSaveOptions(true, "result", "tab", "\t", null);
    }

    private void setAntiPrefixes(List<String> antiPrefixes) {
        if (antiPrefixes != null && antiPrefixes.size() > 0) {
            hasAntiPrefixes = true;
            this.antiPrefixes = antiPrefixes.stream().sorted().collect(Collectors.toList());
            int size = this.antiPrefixes.size();
            Iterator<String> iterator = this.antiPrefixes.iterator();
            String temp = iterator.next();
            while (iterator.hasNext() && size > 0) {
                size--;
                String prefix = iterator.next();
                if (prefix.startsWith(temp)) iterator.remove();
                else temp = prefix;
            }
        }
    }

    private void setUrisAndMap(Map<String, Map<String, String>> urisMap) throws IOException {
        if (urisMap == null || urisMap.size() <= 0) {
            this.urisMap = new HashMap<>();
        } else {
            if (urisMap.containsKey(null)) throw new IOException("uris map can not contains null.");
            this.urisMap = new HashMap<>(threads);
            this.urisMap.putAll(urisMap);
            int size = this.urisMap.size();
            uris = new ArrayList<>();
            Iterator<String> iterator = this.urisMap.keySet().stream().sorted().collect(Collectors.toList()).iterator();
            while (iterator.hasNext() && size > 0) {
                size--;
                String uri = iterator.next();
                if (uri == null || uri.equals("")) {
                    throw new IOException("uris can not contain empty item.");
                } else {
                    uris.add(uri.split("-\\|\\|-")[0]);
                }
            }
        }
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

    protected abstract ITypeConvert<String, T> getNewConverter() throws IOException;

    protected abstract ITypeConvert<T, String> getNewStringConverter() throws IOException;

    boolean checkPrefix(String name) {
        for (String antiPrefix : antiPrefixes) {
            if (name.startsWith(antiPrefix)) return false;
        }
        return true;
    }

    void recordListerByUri(String prefix) {
        Map<String, String> map = urisMap.get(prefix.split("-\\|\\|-")[0]);
        String record = map == null ? "{}" : JsonUtils.toJsonObject(map).toString();
        recordLister(prefix, record);
    }

    public void export(ITextReader reader, IResultOutput saver, ILineProcess<T> processor) throws Exception {
        ITypeConvert<String, T> converter = getNewConverter();
        ITypeConvert<T, String> stringConverter = null;
        if (saveTotal) {
            stringConverter = getNewStringConverter();
            saver.preAddWriter("failed");
        }
        String lastLine = reader.currentEndLine();
        List<String> srcList = null;
        List<T> convertedList;
        List<String> writeList;
        int retry;
        Map<String, String> map = urisMap.get(reader.getName());
        JsonObject json = map != null ? JsonUtils.toJsonObject(map) : (lastLine != null ? new JsonObject() : null);
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
            statistics.addAndGet(srcList.size());
            convertedList = converter.convertToVList(srcList);
            if (converter.errorSize() > 0) saver.writeError(converter.errorLines(), false);
            if (stringConverter != null) {
                writeList = stringConverter.convertToVList(convertedList);
                if (writeList.size() > 0) saver.writeSuccess(String.join("\n", writeList), false);
                if (stringConverter.errorSize() > 0)
                    saver.writeToKey("failed", stringConverter.errorLines(), false);
            }
            // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
            if (processor != null) {
                try {
                    processor.processLine(convertedList);
                } catch (QiniuException e) {
                    // 这里其实逻辑上没有做重试次数的限制，因为返回的 retry 始终大于等于 -1，所以不是必须抛出的异常则会跳过，process 本身会
                    // 保存失败的记录，除非是 process 出现 599 状态码才会抛出异常
                    if (HttpRespUtils.checkException(e, 2) < -1) throw e;
                    if (e.response != null) e.response.close();
                }
            }
            lastLine = reader.currentEndLine();
            json.addProperty("start", lastLine);
            recordLister(reader.getName(), json.toString());
        }
    }

    protected abstract IResultOutput getNewResultSaver(String order) throws IOException;

    private void reading(ITextReader reader) {
        int order = UniOrderUtils.getOrder();
        String orderStr = String.valueOf(order);
        ILineProcess<T> lineProcessor = null;
        IResultOutput saver = null;
        try {
            saver = getNewResultSaver(orderStr);
            saverMap.put(orderStr, saver);
            if (processor != null) {
                lineProcessor = processor.clone();
                lineProcessor.changeSaveOrder(orderStr);
                processorMap.put(orderStr, lineProcessor);
            }
            export(reader, saver, lineProcessor);
            procedureLogger.info("{}-|-", reader.getName());
            progressMap.remove(reader.getName()); // 只有 export 成功情况下才移除 record
        }  catch (QiniuException e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}, {}", reader.getName(), progressMap.get(reader.getName()), e.error(), e);
            if (e.response != null) e.response.close();
        } catch (Throwable e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}", reader.getName(), progressMap.get(reader.getName()), e);
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

    protected abstract ITextReader generateReader(String name) throws IOException;

    protected abstract Stream<ITextReader> getReaders(String path) throws IOException;

    public void export() throws Exception {
        String info = processor == null ? String.join(" ", "read lines from path:", path) :
                String.join(" ", "read lines from path:", path, "and", processor.getProcessName());
        rootLogger.info("{} running...", info);
        rootLogger.info("order\tpath\tquantity");
        showdownHook();
        Stream<ITextReader> readerStream;
        if (uris == null || uris.size() == 0) {
            readerStream = getReaders(FileUtils.convertToRealPath(path));
        } else {
            if (hasAntiPrefixes) {
                uris = uris.parallelStream()
                        .filter(this::checkPrefix)
                        .peek(this::recordListerByUri)
                        .collect(Collectors.toList());
            } else {
                uris.parallelStream().forEach(this::recordListerByUri);
            }
            readerStream = uris.parallelStream().map(uri -> {
                try {
                    return generateReader(uri);
                } catch (IOException e) {
                    errorLogger.error("generate lister failed by {}\t{}", uri, urisMap.get(uri), e);
                    return null;
                }
            });
        }
        try {
            executorPool = Executors.newFixedThreadPool(threads);
            readerStream.filter(generated -> {
                if (generated == null) return false;
                else if (generated.currentEndLine() != null) return true;
                else {
                    progressMap.remove(generated.getName());
                    generated.close();
                    return false;
                }
            }).forEach(reader -> executorPool.execute(() -> reading(reader)));
            executorPool.shutdown();
            while (!executorPool.isTerminated()) {
                sleep(2000);
                if (countInterval-- <= 0) {
                    countInterval = 300;
                    refreshRecordAndStatistics();
                }
            }
            rootLogger.info("{} finished, results in {}.", info, savePath);
            endAction();
        } catch (Throwable e) {
            stopped = true;
            rootLogger.error("export failed", e);
            endAction();
            System.exit(-1);
        }
    }
}
