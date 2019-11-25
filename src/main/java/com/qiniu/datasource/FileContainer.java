package com.qiniu.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.interfaces.*;
import com.qiniu.util.*;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public abstract class FileContainer<E, W, T> extends DatasourceActor implements IDataSource<ILocalFileLister<E, File>, IResultOutput<W>, T> {

    protected String path;
    protected String transferPath = null;
    protected int leftTrimSize = 0;
    protected String realPath;
    protected List<String> antiDirectories;
    protected boolean hasAntiDirectories = false;
    protected Map<String, Map<String, String>> directoriesMap;
    protected List<File> directories;
    protected ILineProcess<T> processor; // 定义的资源处理器
    protected ConcurrentMap<String, ILocalFileLister<E, File>> listerMap = new ConcurrentHashMap<>(threads);

    public FileContainer(String path, Map<String, Map<String, String>> directoriesMap, List<String> antiDirectories,
                         Map<String, String> indexMap, List<String> fields, int unitLen, int threads) throws IOException {
        super(unitLen, threads);
        this.path = path;
        this.antiDirectories = antiDirectories;
        if (antiDirectories != null && antiDirectories.size() > 0) hasAntiDirectories = true;
        setTransferPathAndLeftTrimSize();
        setDirectoriesAndMap(directoriesMap);
        setIndexMapWithDefault(indexMap);
        if (fields != null && fields.size() > 0) this.fields = fields;
        else this.fields = ConvertingUtils.getOrderedFields(this.indexMap, null);
        // default save parameters，默认全记录保存
        setSaveOptions(true, "result", "tab", "\t", null);
    }

    private void setIndexMapWithDefault(Map<String, String> indexMap) throws IOException {
        if (indexMap == null || indexMap.size() == 0) {
            if (this.indexMap == null) this.indexMap = new HashMap<>();
            for (String fileInfoField : ConvertingUtils.defaultFileInfos) {
                this.indexMap.put(fileInfoField, fileInfoField);
            }
        } else {
            for (String s : indexMap.keySet()) {
                if (s == null || "".equals(s)) throw new IOException("the index can not be empty in " + indexMap);
            }
            this.indexMap = indexMap;
        }
    }

    private void setTransferPathAndLeftTrimSize() throws IOException {
        if (path.indexOf(FileUtils.pathSeparator + FileUtils.currentPath) > 0 ||
                path.indexOf(FileUtils.pathSeparator + FileUtils.parentPath) > 0 ||
                path.endsWith(FileUtils.pathSeparator + ".") ||
                path.endsWith(FileUtils.pathSeparator + "..")) {
            throw new IOException("please set straight path, can not contain \"/..\" or \"/.\".");
        } else if (path.startsWith(FileUtils.userHomeStartPath)) {
            realPath = String.join("", FileUtils.userHome, path.substring(1));
            transferPath = "~";
            leftTrimSize = FileUtils.userHome.length();
        } else {
            realPath = path;
            if (path.startsWith(FileUtils.parentPath) || "..".equals(path)) {
                transferPath = "";
                leftTrimSize = 3;
            } else if (path.startsWith(FileUtils.currentPath) || ".".equals(path)) {
                transferPath = "";
                leftTrimSize = 2;
            }
        }
        if (realPath.contains("\\~")) realPath = realPath.replace("\\~", "~");
        if (realPath.endsWith(FileUtils.pathSeparator)) {
            realPath = realPath.substring(0, realPath.length() - 1);
        }
    }

    private void setDirectoriesAndMap(Map<String, Map<String, String>> directoriesMap) throws IOException {
        if (directoriesMap == null || directoriesMap.size() <= 0) {
            this.directoriesMap = new HashMap<>(threads);
        } else {
            if (directoriesMap.containsKey(null)) throw new IOException("can not find directory named \"null\".");
            this.directoriesMap = new HashMap<>(threads);
            this.directoriesMap.putAll(directoriesMap);
            directories = new ArrayList<>();
            int size = this.directoriesMap.size();
            Iterator<String> iterator = this.directoriesMap.keySet().stream().sorted().collect(Collectors.toList()).iterator();
            String temp = iterator.next();
            File tempFile;
            Map<String, String> value = directoriesMap.get(temp);
            String end;
            if (temp == null || temp.equals("")) {
                throw new IOException("directories can not contains empty item");
            } else {
                tempFile = new File(temp);
                if (tempFile.exists() && tempFile.isDirectory()) directories.add(tempFile);
            }
            while (iterator.hasNext() && size > 0) {
                size--;
                String directory = iterator.next();
                if (directory == null || directory.equals("")) {
                    throw new IOException("directories can not contains empty item");
                } else {
                    File file = new File(directory);
                    if (file.isDirectory()) {
                        if (tempFile.isDirectory()) {
                            if (file.getPath().startsWith(tempFile.getPath())) {
                                end = value == null ? null : value.get("end");
                                if (end == null || "".equals(end)) {
                                    iterator.remove();
                                    this.directoriesMap.remove(directory);
                                } else if (end.compareTo(directory) >= 0) {
                                    throw new IOException(temp + "'s end can not be more larger than " + directory + " in " + directoriesMap);
                                } else {
                                    directories.add(file);
                                }
                            } else {
                                directories.add(file);
                            }
                        } else {
                            directories.add(file);
                            tempFile = file;
                            temp = directory;
                            value = directoriesMap.get(temp);
                        }

                    }
                }
            }
        }
    }

    public void setProcessor(ILineProcess<T> processor) {
        this.processor = processor;
    }

    protected abstract ITypeConvert<E, T> getNewConverter() throws IOException;

    protected abstract ITypeConvert<E, String> getNewStringConverter() throws IOException;

    private boolean checkDirectory(File directory) {
        if (hasAntiDirectories) {
            for (String antiPrefix : antiDirectories) {
                if (directory.getPath().startsWith(antiPrefix)) return false;
            }
            return true;
        } else {
            return true;
        }
    }

    protected abstract ILocalFileLister<E, File> getLister(File directory, String start, String end, int unitLen) throws IOException;

    ILocalFileLister<E, File> generateLister(File directory) throws IOException {
        return generateLister(directory, 0);
    }

    private ILocalFileLister<E, File> generateLister(File directory, int limit) throws IOException {
        limit = limit > 0 ? limit : unitLen;
        int retry = retryTimes;
        Map<String, String> map = directoriesMap.get(directory.getPath());
        String start;
        String end;
        if (map == null) {
            start = end = null;
        } else {
            start = map.get("start");
            end = map.get("end");
        }
        while (true) {
            try {
                return getLister(directory, start, end, limit);
            } catch (SuitsException e) {
                retry = HttpRespUtils.listExceptionWithRetry(e, retry);
                try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                errorLogger.error("generate lister by directory:{} retrying...", directory, e);
            }
        }
    }

    public void export(ILocalFileLister<E, File> lister, IResultOutput<W> saver, ILineProcess<T> processor) throws Exception {
        ITypeConvert<E, T> converter = getNewConverter();
        ITypeConvert<E, String> stringConverter = null;
        if (saveTotal) {
            stringConverter = getNewStringConverter();
            saver.preAddWriter("failed");
        }
        List<T> convertedList;
        List<String> writeList;
        List<E> objects = lister.currents();
        boolean hasNext = lister.hasNext();
        Map<String, String> map = directoriesMap.get(lister.getName());
        // 初始化的 lister 包含首次列举的结果列表，需要先取出，后续向前列举时会更新其结果列表
        while (objects.size() > 0 || hasNext) {
            if (stopped) break;
            if (LocalDateTime.now(DatetimeUtils.clock_Default).isAfter(pauseDateTime)) {
                synchronized (object) {
                    object.wait();
                }
            }
            if (stringConverter != null) {
                writeList = stringConverter.convertToVList(objects);
                if (writeList.size() > 0) saver.writeSuccess(String.join("\n", writeList), false);
                if (stringConverter.errorSize() > 0) saver.writeToKey("failed", stringConverter.errorLines(), false);
            }
            if (processor != null) {
                convertedList = converter.convertToVList(objects);
                if (converter.errorSize() > 0) saver.writeError(converter.errorLines(), false);
                // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
                try {
                    processor.processLine(convertedList);
                } catch (QiniuException e) {
                    if (HttpRespUtils.checkException(e, 2) < -1) throw e;
                    errorLogger.error("process objects: {}", lister.getName(), e);
                    if (e.response != null) e.response.close();
                }
            }
            if (hasNext) {
                JsonObject json = recorder.getOrDefault(lister.getName(), new JsonObject());
                json.addProperty("end", lister.getEndPrefix());
                try { FileUtils.createIfNotExists(procedureLogFile); } catch (IOException ignored) {}
                procedureLogger.info(recorder.put(lister.getName(), json));
            }
            if (map != null) map.put("start", lister.currentEndFilepath());
            if (stopped) break;
//            objects.clear(); 上次其实不能做 clear，会导致 lister 中的列表被清空
            lister.listForward();
            objects = lister.currents();
            hasNext = lister.hasNext();
        }
    }

    protected abstract IResultOutput<W> getNewResultSaver(String order) throws IOException;

    private void listing(ILocalFileLister<E, File> lister) {
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
            export(lister, saver, lineProcessor);
            recorder.remove(lister.getName());
        }  catch (QiniuException e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}, {}", lister.getName(), recorder.getString(lister.getName()), e.error(), e);
            if (e.response != null) e.response.close();
        } catch (Throwable e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}", lister.getName(), recorder.getString(lister.getName()), e);
        } finally {
            try { FileUtils.createIfNotExists(infoLogFile); } catch (IOException ignored) {}
            infoLogger.info("{}\t{}\t{}", orderStr, lister.getName(), lister.count());
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
            lister.close();
        }
    }

    private void recordListerByDirectory(String directory) {
        JsonObject json = directoriesMap.get(directory) == null ? null : JsonUtils.toJsonObject(directoriesMap.get(directory));
        try { FileUtils.createIfNotExists(procedureLogFile); } catch (IOException ignored) {}
        procedureLogger.info(recorder.put(directory, json));
    }

    private List<File> directoriesAfterListerRun(File directory) {
        try {
            ILocalFileLister<E, File> lister = generateLister(directory);
            if (lister.hasNext() || lister.getDirectories() != null) {
                listing(lister);
                if (lister.getDirectories() == null || lister.getDirectories().size() <= 0) {
                    return null;
                } else if (hasAntiDirectories) {
                    return lister.getDirectories().stream().filter(this::checkDirectory)
                            .peek(dir -> recordListerByDirectory(dir.getPath())).collect(Collectors.toList());
                } else {
                    for (File dir : lister.getDirectories()) recordListerByDirectory(dir.getPath());
                    return lister.getDirectories();
                }
            } else {
                listing(lister);
                return lister.getDirectories();
            }
        } catch (IOException e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("generate lister failed by {}\t{}", directory.getPath(), directoriesMap.get(directory.getPath()), e);
            return null;
        }
    }

    private AtomicLong atomicLong = new AtomicLong(0);

    private void listForNextIteratively(List<File> directories) throws Exception {
        List<File> tempPrefixes;
        List<Future<List<File>>> futures = new ArrayList<>();
        for (File directory : directories) {
            if (atomicLong.get() > threads) {
                tempPrefixes = directoriesAfterListerRun(directory);
                if (tempPrefixes != null) listForNextIteratively(tempPrefixes);
            } else {
                atomicLong.incrementAndGet();
                futures.add(executorPool.submit(() -> {
                    List<File> list = directoriesAfterListerRun(directory);
                    atomicLong.decrementAndGet();
                    return list;
                }));
            }
        }
        Iterator<Future<List<File>>> iterator;
        Future<List<File>> future;
        while (futures.size() > 0) {
            iterator = futures.iterator();
            while (iterator.hasNext()) {
                future = iterator.next();
                if (future.isDone()) {
                    tempPrefixes = future.get();
                    if (tempPrefixes != null) listForNextIteratively(tempPrefixes);
                    iterator.remove();
                }
            }
        }
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    @Override
    public void export() throws Exception {
        String info = processor == null ?
                String.join(" ", "list files from path:", path) :
                String.join(" ", "read files from path:", path, "and", processor.getProcessName());
        rootLogger.info("{} running...", info);
        showdownHook();
        if (directories == null || directories.size() == 0) {
            ILocalFileLister<E, File> fileInfoLister = generateLister(new File(realPath));
            if (fileInfoLister.currents().size() > 0 || fileInfoLister.hasNext()) {
                listing(fileInfoLister);
            }
            if (fileInfoLister.getDirectories() == null || fileInfoLister.getDirectories().size() <= 0) {
                directories = null;
            } else if (hasAntiDirectories) {
                directories = fileInfoLister.getDirectories().parallelStream().filter(this::checkDirectory)
                        .peek(directory -> recordListerByDirectory(directory.getPath())).collect(Collectors.toList());
            } else {
                for (File dir : fileInfoLister.getDirectories()) recordListerByDirectory(dir.getPath());
                directories = fileInfoLister.getDirectories();
            }
        }
        try {
            if (directories != null && directories.size() > 0) {
                executorPool = Executors.newFixedThreadPool(threads);
                listForNextIteratively(directories);
                executorPool.shutdown();
                while (!executorPool.isTerminated()) {
                    sleep(1000);
                }
            }
            rootLogger.info("{} finished.", info);
            endAction();
        } catch (Throwable e) {
            stopped = true;
            rootLogger.error(e.toString(), e);
            endAction();
            System.exit(-1);
        }
    }
}
