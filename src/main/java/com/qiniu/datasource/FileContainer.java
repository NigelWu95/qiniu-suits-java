package com.qiniu.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.JsonRecorder;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.*;
import com.qiniu.model.local.FileInfo;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.qiniu.entry.CommonParams.lineFormats;

public abstract class FileContainer<W, T> implements IDataSource<FileInfoLister, IResultOutput<W>, T> {

    static final File errorLogFile = new File(String.join(".", LogUtils.getLogPath(LogUtils.QSUITS), LogUtils.ERROR));
    static final File infoLogFile = new File(String.join(".", LogUtils.getLogPath(LogUtils.QSUITS), LogUtils.INFO));
    static final File procedureLogFile = new File(String.join(".", LogUtils.getLogPath(LogUtils.PROCEDURE), LogUtils.LOG_EXT));
    static final Logger rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger errorLogger = LoggerFactory.getLogger(LogUtils.ERROR);
    static final Logger infoLogger = LoggerFactory.getLogger(LogUtils.INFO);
    static final Logger procedureLogger = LoggerFactory.getLogger(LogUtils.PROCEDURE);

    protected String path;
    protected List<FileInfo> totalFileInfoList;
    protected String transferPath = null;
    protected int leftTrimSize = 0;
    protected String realPath;
    protected List<String> antiDirectories;
    protected boolean hasAntiDirectories = false;
    protected Map<String, Map<String, String>> directoriesMap;
    protected List<File> directories;
    protected Map<String, String> indexMap;
    protected int unitLen;
    protected int threads;
    protected int retryTimes = 5;
    protected String savePath;
    protected boolean saveTotal;
    protected String saveFormat;
    protected String saveSeparator;
    protected List<String> rmFields;
    protected List<String> fields;
    protected ExecutorService executorPool;
    protected ILineProcess<T> processor; // 定义的资源处理器
    protected ConcurrentMap<String, IResultOutput<W>> saverMap = new ConcurrentHashMap<>(threads);
    protected ConcurrentMap<String, ILineProcess<T>> processorMap = new ConcurrentHashMap<>(threads);
    private boolean stopped;

    public FileContainer(String path, Map<String, Map<String, String>> directoriesMap, List<String> antiDirectories,
                         Map<String, String> indexMap, List<String> fields, int unitLen, int threads) throws IOException {
        this.path = path;
        this.antiDirectories = antiDirectories;
        if (antiDirectories != null && antiDirectories.size() > 0) hasAntiDirectories = true;
        setTransferPathAndLeftTrimSize();
        setDirectoriesAndMap(directoriesMap);
        this.indexMap = indexMap;
        this.unitLen = unitLen;
        this.threads = threads;
        // default save parameters
        this.saveTotal = false; // 默认全记录不保存
        this.savePath = "result";
        this.saveFormat = "tab";
        this.saveSeparator = "\t";
        if (fields == null || fields.size() == 0) {
            this.fields = ConvertingUtils.getOrderedFields(this.indexMap, rmFields);
        }
        else this.fields = fields;
    }

    // 不调用则各参数使用默认值
    public void setSaveOptions(boolean saveTotal, String savePath, String format, String separator, List<String> rmFields)
            throws IOException {
        this.saveTotal = saveTotal;
        this.savePath = savePath;
        this.saveFormat = format;
        if (!lineFormats.contains(saveFormat)) throw new IOException("please check your format for map to string.");
        this.saveSeparator = separator;
        this.rmFields = rmFields;
        if (rmFields != null && rmFields.size() > 0) {
            this.fields = ConvertingUtils.getFields(fields, rmFields);
        }
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public void setProcessor(ILineProcess<T> processor) {
        this.processor = processor;
    }

    protected abstract ITypeConvert<FileInfo, T> getNewConverter() throws IOException;

    protected abstract ITypeConvert<FileInfo, String> getNewStringConverter() throws IOException;

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

    private JsonRecorder recorder = new JsonRecorder();

    public void export(FileInfoLister lister, IResultOutput<W> saver, ILineProcess<T> processor) throws Exception {
        ITypeConvert<FileInfo, T> converter = getNewConverter();
        ITypeConvert<FileInfo, String> stringConverter = null;
        if (saveTotal) {
            stringConverter = getNewStringConverter();
            saver.preAddWriter("failed");
        }
        List<T> convertedList;
        List<String> writeList;
        List<FileInfo> objects = lister.currents();
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
            if (map != null) map.put("start", lister.currentEndKey());
            if (stopped) break;
//            objects.clear(); 上次其实不能做 clear，会导致 lister 中的列表被清空
            lister.listForward();
            objects = lister.currents();
            hasNext = lister.hasNext();
        }
    }

    protected abstract IResultOutput<W> getNewResultSaver(String order) throws IOException;

    private void listing(FileInfoLister lister) {
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

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
            int i = 0;
            while (i < millis) i++;
        }
    }

    private void endAction() throws IOException {
        ILineProcess<T> processor;
        for (Map.Entry<String, IResultOutput<W>> saverEntry : saverMap.entrySet()) {
            saverEntry.getValue().closeWriters();
            processor = processorMap.get(saverEntry.getKey());
            if (processor != null) processor.closeResource();
        }
        String record = recorder.toString();
        if (recorder.size() > 0) {
            String path = new File(savePath).getCanonicalPath();
            FileSaveMapper saveMapper = new FileSaveMapper(new File(path).getParent());
            saveMapper.setAppend(false);
            saveMapper.setFileExt(".json");
            String fileName = path.substring(path.lastIndexOf(FileUtils.pathSeparator) + 1) + "-lines";
            saveMapper.addWriter(fileName);
            saveMapper.writeToKey(fileName, record, true);
            saveMapper.closeWriters();
            rootLogger.info("please check the lines breakpoint in {}.json, " +
                            "it can be used for one more time reading remained lines", fileName);
        }
        procedureLogger.info(record);
    }

    private void showdownHook() {
        SignalHandler handler = signal -> {
            try {
                stopped = true;
                endAction();
            } catch (IOException e) {
                rootLogger.error("showdown error", e);
            }
            System.exit(0);
        };
        try { // 设置 INT 信号 (Ctrl + C 中断执行) 交给指定的信号处理器处理，废掉系统自带的功能
            Signal.handle(new Signal("INT"), handler); } catch (Exception ignored) {}
        try { Signal.handle(new Signal("TERM"), handler); } catch (Exception ignored) {}
        try { Signal.handle(new Signal("USR1"), handler); } catch (Exception ignored) {}
        try { Signal.handle(new Signal("USR2"), handler); } catch (Exception ignored) {}
    }

    private void recordListerByDirectory(String directory) {
        JsonObject json = directoriesMap.get(directory) == null ? null : JsonUtils.toJsonObject(directoriesMap.get(directory));
        try { FileUtils.createIfNotExists(procedureLogFile); } catch (IOException ignored) {}
        procedureLogger.info(recorder.put(directory, json));
    }

    private List<File> directoriesAfterListerRun(File directory) {
        try {
            FileInfoLister fileInfoLister = new FileInfoLister(directory, true, transferPath, leftTrimSize, null, null, unitLen);
            if (fileInfoLister.hasNext() || fileInfoLister.getDirectories() != null) {
                listing(fileInfoLister);
                if (fileInfoLister.getDirectories() == null || fileInfoLister.getDirectories().size() <= 0) {
                    return null;
                } else if (hasAntiDirectories) {
                    return fileInfoLister.getDirectories().stream().filter(this::checkDirectory)
                            .peek(dir -> recordListerByDirectory(dir.getPath())).collect(Collectors.toList());
                } else {
                    for (File dir : fileInfoLister.getDirectories()) recordListerByDirectory(dir.getPath());
                    return fileInfoLister.getDirectories();
                }
            } else {
                listing(fileInfoLister);
                return fileInfoLister.getDirectories();
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
            FileInfoLister fileInfoLister = new FileInfoLister(new File(realPath), true, transferPath, leftTrimSize, null, null, unitLen);
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

    private final Object object = new Object();
    private LocalDateTime pauseDateTime = LocalDateTime.MAX;

    public void export(LocalDateTime startTime, long pauseDelay, long duration) throws Exception {
        if (startTime != null) {
            Clock clock = Clock.systemDefaultZone();
            LocalDateTime now = LocalDateTime.now(clock);
            if (startTime.minusWeeks(1).isAfter(now)) {
                throw new Exception("startTime is not allowed to exceed next week");
            }
            while (now.isBefore(startTime)) {
                System.out.printf("\r%s", LocalDateTime.now(clock).toString().substring(0, 19));
                sleep(1000);
                now = LocalDateTime.now(clock);
            }
        }
        if (duration <= 0 || pauseDelay < 0) {
            export();
        } else if (duration > 84600 || duration < 1800) {
            throw new Exception("duration can not be bigger than 23.5 hours or smaller than 0.5 hours.");
        } else {
            pauseDateTime = LocalDateTime.now().plusSeconds(pauseDelay);
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    synchronized (object) {
                        object.notifyAll();
                    }
                    pauseDateTime = LocalDateTime.now().plusSeconds(86400 - duration);
//                    pauseDateTime = LocalDateTime.now().plusSeconds(20 - duration);
                }
            }, (pauseDelay + duration) * 1000, 86400000);
//            }, (pauseDelay + duration) * 1000, 20000);
            export();
            timer.cancel();
        }
    }
}
