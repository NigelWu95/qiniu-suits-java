package com.qiniu.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.*;
import com.qiniu.util.*;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public abstract class FileContainer<E, W, T> extends DatasourceActor implements IDataSource<ILocalFileLister<E, File>, IResultOutput<W>, T> {

    protected String path;
    protected boolean keepDir;
    protected String transferPath = null;
    protected int leftTrimSize = 0;
    protected String realPath;
    protected int initPathSize;
    protected List<String> antiPrefixes;
    protected boolean hasAntiPrefixes = false;
    protected Map<String, Map<String, String>> prefixesMap;
    protected List<File> directories;
    protected ILineProcess<T> processor; // 定义的资源处理器
//    protected List<ILocalFileLister<E, File>> listerList = new ArrayList<>(threads);
    protected ConcurrentMap<String, ILocalFileLister<E, File>> listerMap = new ConcurrentHashMap<>(threads);
    protected boolean withEtag;
    protected boolean withDatetime;
    protected boolean withMime;
    protected boolean withParent;
    private ILocalFileLister<E, File> startFileInfoLister = null;

    public FileContainer(String path, Map<String, Map<String, String>> prefixesMap, List<String> antiDirectories, boolean keepDir,
                         Map<String, String> indexMap, List<String> fields, int unitLen, int threads) throws IOException {
        super(unitLen, threads);
        this.path = path;
        this.keepDir = keepDir;
        setIndexMapWithDefault(indexMap);
        setAntiDirectories(antiDirectories);
        setTransferPathAndLeftTrimSize();
        setDirectoriesAndMap(prefixesMap);
        if (fields != null && fields.size() > 0) this.fields = fields;
        else this.fields = ConvertingUtils.getOrderedFields(this.indexMap, null);
        // default save parameters，默认全记录保存
        setSaveOptions(true, "result", "tab", "\t", null);
    }

    private void setAntiDirectories(List<String> antiPrefixes) {
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
        initPathSize = realPath.split(FileUtils.pathSeparator).length;
    }

    protected abstract String getNameWithoutParent(E e);

    private void setDirectoriesAndMap(Map<String, Map<String, String>> prefixesMap) throws IOException {
        if (prefixesMap == null || prefixesMap.size() <= 0) {
            this.prefixesMap = new HashMap<>();
            File originFile = new File(realPath);
            recordListerByDirectory(realPath);
            if (originFile.isDirectory()) startFileInfoLister = generateLister(originFile);
            else startFileInfoLister = getLister(realPath);
            directories = startFileInfoLister.getDirectories();
        } else {
            if (prefixesMap.containsKey(null)) throw new IOException("prefixes map can not contains null.");
            this.prefixesMap = new HashMap<>(threads);
            this.prefixesMap.putAll(prefixesMap);
            int size = this.prefixesMap.size();
            Iterator<String> iterator = this.prefixesMap.keySet().stream()
                    .map(directory -> directory = directory.split("-\\|\\|-")[0])
                    .sorted().collect(Collectors.toList()).iterator();
            File originFile = new File(realPath);
            recordListerByDirectory(realPath);
            if (originFile.isDirectory()) startFileInfoLister = generateLister(originFile);
            else startFileInfoLister = getLister(realPath);
            String temp = iterator.next();
            Map<String, String> value = prefixesMap.get(temp);
            String end;
            if (temp.equals("")) throw new IOException("file prefixes can not only be empty string(\"\")");
            File file;
            File tempFile = new File(temp);
            while (iterator.hasNext() && size > 0) {
                size--;
                String prefix = iterator.next();
                if (prefix == null || prefix.equals("")) {
                    throw new IOException("file prefixes can not contains empty item");
                } else {
                    file = new File(realPath, prefix);
                    if (file.isDirectory()) {
                        if (tempFile.isDirectory()) {
                            if (file.getPath().startsWith(tempFile.getPath() + FileUtils.pathSeparator)) {
                                end = value == null ? null : value.get("end");
                                if (end == null || "".equals(end)) {
                                    iterator.remove();
                                    this.prefixesMap.remove(prefix);
                                } else if (end.compareTo(prefix) >= 0) {
                                    throw new IOException(temp + "'s end can not be larger than " + prefix + " in " + prefixesMap);
                                }
                            }
                        } else {
                            tempFile = file;
                            temp = prefix;
                            value = prefixesMap.get(temp);
                        }
                    }
                }
            }
            directories = startFileInfoLister.getDirectories().parallelStream()
                    .filter(directory -> prefixesMap.keySet().stream().anyMatch(directory.getName()::startsWith))
                    .collect(Collectors.toList());
            if (startFileInfoLister.hasNext()) {
                startFileInfoLister = getLister(originFile.getName(), startFileInfoLister.getRemainedFiles().parallelStream()
                        .filter(e -> prefixesMap.keySet().stream().anyMatch(getNameWithoutParent(e)::startsWith))
                        .collect(Collectors.toList()), null, null, unitLen);
            }
        }
    }

    private void setIndexMapWithDefault(Map<String, String> indexMap) throws IOException {
        if (indexMap == null || indexMap.size() == 0) {
            if (this.indexMap == null) this.indexMap = new HashMap<>();
            for (String fileInfoField : ConvertingUtils.localFileInfoFields) {
                this.indexMap.put(fileInfoField, fileInfoField);
            }
        } else {
            for (String s : indexMap.keySet()) {
                if (s == null || "".equals(s)) throw new IOException("the index can not be empty in " + indexMap);
            }
            this.indexMap = indexMap;
        }
        withEtag = this.indexMap.containsKey("etag");
        withDatetime = this.indexMap.containsKey("datetime");
        withMime = this.indexMap.containsKey("mime");
        withParent = this.indexMap.containsKey("parent");
    }

    public void setProcessor(ILineProcess<T> processor) {
        this.processor = processor;
    }

    protected abstract ITypeConvert<E, T> getNewConverter() throws IOException;

    protected abstract ITypeConvert<E, String> getNewStringConverter() throws IOException;

    private boolean checkPrefix(File directory) {
        for (String antiPrefix : antiPrefixes) {
            if (directory.getPath().startsWith(antiPrefix)) return false;
        }
        return true;
    }

    protected abstract ILocalFileLister<E, File> getLister(File directory, String start, String end, int unitLen) throws IOException;

    protected abstract ILocalFileLister<E, File> getLister(String name, List<E> fileInfoList, String start,
                                                           String end, int unitLen) throws IOException;

    protected abstract ILocalFileLister<E, File> getLister(String singleFilePath) throws IOException;

    private ILocalFileLister<E, File> generateLister(File directory) throws IOException {
        Map<String, String> map = prefixesMap.get(directory.getPath());
        String start;
        String end;
        if (map == null) {
            start = end = null;
        } else {
            start = map.get("start");
            end = map.get("end");
        }
        return getLister(directory, start, end, unitLen);
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
        Map<String, String> map = prefixesMap.get(lister.getName());
        JsonObject jsonObject = map == null ? new JsonObject() : JsonUtils.toJsonObject(map);
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
                jsonObject.addProperty("end", lister.getEndPrefix());
                jsonObject.addProperty("start", lister.currentEndFilepath());
                try { FileUtils.createIfNotExists(procedureLogFile); } catch (IOException ignored) {}
                procedureLogger.info("{}: {}", lister.getName(), jsonObject);
            }
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
            procedureLogger.info("{}: done", lister.getName());
        }  catch (QiniuException e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}, {}", lister.getName(), e.error(), e);
            if (e.response != null) e.response.close();
        } catch (Throwable e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}", lister.getName(), e);
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
            listerMap.remove(lister.getName());
        }
    }

    private void recordListerByDirectory(String name) {
        String pName = name.split("-\\|\\|-")[0];
        Map<String, String> map = prefixesMap.get(pName);
        JsonObject json = map == null ? null : JsonUtils.toJsonObject(map);
        try { FileUtils.createIfNotExists(procedureLogFile); } catch (IOException ignored) {}
        procedureLogger.info("{}: {}", name, json);
    }

    private void processNodeLister(ILocalFileLister<E, File> lister) {
        if (lister.hasNext()) {
            listerMap.put(lister.getName(), lister);
            integer.incrementAndGet();
            executorPool.execute(() -> {
                listing(lister);
                integer.decrementAndGet();
            });
        } else {
            lister.close();
        }
    }

    private List<File> loopForFutures(List<Future<ILocalFileLister<E, File>>> futures) throws Exception {
        Iterator<Future<ILocalFileLister<E, File>>> iterator;
        Future<ILocalFileLister<E, File>> future;
        ILocalFileLister<E, File> tempLister;
        List<File> nextDirectories = new ArrayList<>();
//            iterator = futures.iterator();
//            while (iterator.hasNext()) {
//                future = iterator.next();
//                if (future.isDone()) {
//                    tempLister = future.get();
//                    if (tempLister != null) {
//                        processNodeLister(tempLister);
//                        if (tempLister.getDirectories() != null && tempLister.getDirectories().size() > 0) {
//                            listForNextIteratively(tempLister.getDirectories());
//                        }
//                    }
//                    iterator.remove();
//                }
//            }
//        List<File> nextDirectories = new ArrayList<>();
        iterator = futures.iterator();
        while (iterator.hasNext()) {
            future = iterator.next();
            if (future.isDone()) {
                tempLister = future.get();
                if (tempLister != null) {
                    if (tempLister.getDirectories() != null && tempLister.getDirectories().size() > 0) {
                        if (hasAntiPrefixes) {
                            nextDirectories.addAll(tempLister.getDirectories().parallelStream()
                                    .filter(this::checkPrefix)
                                    .peek(dir -> recordListerByDirectory(dir.getPath()))
                                    .collect(Collectors.toList()));
                        } else {
                            tempLister.getDirectories().parallelStream().forEach(dir ->
                                    recordListerByDirectory(dir.getPath()));
                            nextDirectories.addAll(tempLister.getDirectories());
                        }
                        tempLister.getDirectories().clear();
                    }
                    processNodeLister(tempLister);
                }
                integer.decrementAndGet();
                iterator.remove();
            }
        }
        iterator = null;
        future = null;
        tempLister = null;
        return nextDirectories;
    }

    private Lock lock = new ReentrantLock();
    private AtomicInteger integer = new AtomicInteger(threads);

    private List<File> listForNextIteratively(List<File> directories) throws Exception {
        List<Future<ILocalFileLister<E, File>>> futures = new ArrayList<>();
        List<File> nextDirectories = new ArrayList<>();
        Future<ILocalFileLister<E, File>> future;
        List<File> tempDirectories;
        for (File directory : directories) {
            if (integer.get() < threads) {
                future = executorPool.submit(() -> {
                    try {
                        return generateLister(directory);
                    } catch (IOException e) {
                        try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                        errorLogger.error("generate lister failed by {}\t{}", directory.getPath(), prefixesMap.get(directory.getPath()), e);
                        return null;
                    }
                });
                if (future.isDone() && lock.tryLock()) {
                    try {
                        ILocalFileLister<E, File> futureLister = future.get();
                        if (futureLister != null) {
                            if (futureLister.getDirectories() != null && futureLister.getDirectories().size() > 0) {
                                if (hasAntiPrefixes) {
                                    nextDirectories.addAll(futureLister.getDirectories().parallelStream()
                                            .filter(this::checkPrefix)
                                            .peek(dir -> recordListerByDirectory(dir.getPath()))
                                            .collect(Collectors.toList()));
                                } else {
                                    futureLister.getDirectories().parallelStream().forEach(dir ->
                                            recordListerByDirectory(dir.getPath()));
                                    nextDirectories.addAll(futureLister.getDirectories());
                                }
                                futureLister.getDirectories().clear();
                            }
                            processNodeLister(futureLister);
                        }
                    } catch (Exception e) {
                        try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                        errorLogger.error("excute lister failed", e);
                    } finally {
                        lock.unlock();
                    }
                } else {
                    integer.incrementAndGet();
                    futures.add(future);
                }
            } else {
                while (!lock.tryLock());
                try {
                    ILocalFileLister<E, File> futureLister = generateLister(directory);
                    if (futureLister.getDirectories() != null && futureLister.getDirectories().size() > 0) {
                        if (hasAntiPrefixes) {
                            nextDirectories.addAll(futureLister.getDirectories().parallelStream()
                                    .filter(this::checkPrefix)
                                    .peek(dir -> recordListerByDirectory(dir.getPath()))
                                    .collect(Collectors.toList()));
                        } else {
                            futureLister.getDirectories().parallelStream().forEach(dir ->
                                    recordListerByDirectory(dir.getPath()));
                            nextDirectories.addAll(futureLister.getDirectories());
                        }
                        futureLister.getDirectories().clear();
                    }
                    processNodeLister(futureLister);
                } catch (Exception e) {
                    try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                    errorLogger.error("generate lister failed by {}\t{}", directory.getPath(), prefixesMap.get(directory.getPath()), e);
                } finally {
                    lock.unlock();
                }
            }
            tempDirectories = loopForFutures(futures);
            nextDirectories.addAll(tempDirectories);
            tempDirectories.clear();
        }
        while (futures.size() > 0) {
            tempDirectories = loopForFutures(futures);
            nextDirectories.addAll(loopForFutures(futures));
            tempDirectories.clear();
        }
        futures = null;
        future = null;
        tempDirectories = null;
        directories.clear();
        directories = null;
        return nextDirectories;
    }

    private List<ILocalFileLister<E, File>> checkListerInPool(int cValue, int tiny) {
        int count = 0;
        ILocalFileLister<E, File> iLister;
        boolean notCheck = true;
        List<ILocalFileLister<E, File>> list = new ArrayList<>(listerMap.values());
        Iterator<ILocalFileLister<E, File>> iterator = list.iterator();
        String directory;
        String start;
        Map<String, String> endMap;
        while (!executorPool.isTerminated()) {
            if (count >= 1200) {
                notCheck = false;
                while (iterator.hasNext()) {
                    iLister = iterator.next();
                    if(!iLister.hasNext()) iterator.remove();
                }
                if (list.size() > 0 && list.size() <= tiny) {
                    rootLogger.info("unfinished: {}, cValue: {}, to re-split lister list...\n", list.size(), cValue);
                    for (ILocalFileLister<E, File> lister : list) {
                        // lister 的 prefix 为 final 对象，不能因为 truncate 的操作之后被修改
                        directory = lister.getName();
                        start = lister.truncate();
                        endMap = prefixesMap.get(directory);
                        if (endMap == null) endMap = new HashMap<>();
                        endMap.put("start", start);
                        rootLogger.info("directory: {}, nextFilepath: {}, endMap: {}\n", directory, start, endMap);
                    }
                } else if (list.size() <= cValue) {
                    count = 900;
                } else {
                    count = 0;
                }
            }
            sleep(1000);
            count++;
        }
        if (notCheck) return new ArrayList<>();
        else return list;
    }

    private void directoriesListing() throws Exception {
//        while (directories != null && directories.size() > 0) {
//            directories = directories.parallelStream().map(this::directoriesFromLister).filter(Objects::nonNull)
//                    .reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(null);
//        }
        while (directories != null && directories.size() > 0) directories = listForNextIteratively(directories);
        executorPool.shutdown();
        if (threads > 1) {
            int cValue = threads >= 10 ? threads / 2 : 3;
            int tiny = threads >= 300 ? 30 : threads >= 200 ? 20 : threads >= 100 ? 10 : threads >= 30 ? threads / 10 :
                    threads >= 10 ? 3 : 1;
            List<ILocalFileLister<E, File>> list = checkListerInPool(cValue, tiny);
            while (list.size() > 0) {
                list.parallelStream().forEach(lister -> recordListerByDirectory(lister.getName() + "-||-0"));
                int multiple = threads / list.size();
                int maxIndex = multiple - 1;
                executorPool = Executors.newFixedThreadPool(threads);
                listerMap.clear();
                list.parallelStream().forEach(lister -> {
                    if (lister.getRemainedFiles() == null) return;
                    int remainedSize = lister.getRemainedFiles().size();
                    if (remainedSize < multiple) {
                        if (remainedSize > 0) {
                            try {
                                ILocalFileLister<E, File> sLister = getLister(lister.getName() + "-||-0",
                                        lister.getRemainedFiles(), null, null, unitLen);
                                listerMap.put(sLister.getName(), sLister);
                                executorPool.execute(() -> listing(lister));
                            } catch (IOException e) {
                                try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                                errorLogger.error("generate lister failed by {}\t{}", lister.getName(),
                                        prefixesMap.get(lister.getName()), e);
                            }
                        }
                        return;
                    }
                    int size = remainedSize % multiple == 0 ? remainedSize / multiple : remainedSize / multiple + 1;
                    for (int i = 0; i < multiple; i++) {
                        try {
                            ILocalFileLister<E, File> sLister = getLister(String.join("-||-", lister.getName(), String.valueOf(i)),
                                    lister.getRemainedFiles().subList(size * i, i == maxIndex ? remainedSize : size * (i + 1)),
                                    null, null, unitLen);
                            listerMap.put(sLister.getName(), sLister);
                            executorPool.execute(() -> listing(lister));
                        } catch (IOException e) {
                            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                            errorLogger.error("generate lister failed by {}\t{}",
                                    String.join("-||-", lister.getName(), String.valueOf(i)),
                                    prefixesMap.get(lister.getName()), e);
                        }
                    }
                });
                executorPool.shutdown();
                list = checkListerInPool(cValue, tiny);
            }
        }
        while (!executorPool.isTerminated()) sleep(1000);
    }

    @Override
    public void export() throws Exception {
        String info = processor == null ? String.join(" ", "list files from path:", path) :
                String.join(" ", "read files from path: ", path, "and", processor.getProcessName());
        rootLogger.info("{} running...", info);
        rootLogger.info("order\tprefix\tquantity");
        showdownHook();
        try {
            if (directories == null || directories.size() == 0) {
                if (startFileInfoLister != null) {
                    if (startFileInfoLister.hasNext()) {
                        listing(startFileInfoLister);
                    } else {
                        startFileInfoLister.close();
                    }
                }
            } else {
                if (hasAntiPrefixes) {
                    directories = directories.parallelStream().filter(this::checkPrefix)
                            .peek(directory -> recordListerByDirectory(directory.getPath())).collect(Collectors.toList());
                } else {
                    directories.parallelStream().forEach(directory -> recordListerByDirectory(directory.getPath()));
                }
                executorPool = Executors.newFixedThreadPool(threads);
                if (startFileInfoLister != null) processNodeLister(startFileInfoLister);
                directoriesListing();
            }
            rootLogger.info("{} finished, results in {}.", info, savePath);
            endAction();
        } catch (Throwable e) {
            stopped = true;
            rootLogger.error(e.toString(), e);
            endAction();
            System.exit(-1);
        }
    }
}
