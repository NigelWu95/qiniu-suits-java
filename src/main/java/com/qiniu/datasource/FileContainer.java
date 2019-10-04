package com.qiniu.datasource;

import com.qiniu.common.JsonRecorder;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IDataSource;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.IReader;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.interfaces.IResultOutput;
import com.qiniu.util.FileUtils;
import com.qiniu.util.HttpRespUtils;
import com.qiniu.util.ConvertingUtils;
import com.qiniu.util.UniOrderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.qiniu.entry.CommonParams.lineFormats;

public abstract class FileContainer<E, W, T> implements IDataSource<IReader<E>, IResultOutput<W>, T> {

    private static final Logger rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private static final Logger errorLogger = LoggerFactory.getLogger("error");
    private static final File errorLogFile = new File("qsuits.error");
    private static final Logger infoLogger = LoggerFactory.getLogger("info");
    private static final File infoLogFile = new File("qsuits.info");
    private static final Logger procedureLogger = LoggerFactory.getLogger("procedure");
    private static final File procedureLogFile = new File("procedure.log");

    private String filePath;
    protected String parse;
    protected String separator;
    protected String addKeyPrefix;
    protected String rmKeyPrefix;
    protected Map<String, String> linesMap;
    protected Map<String, String> indexMap;
    protected int unitLen;
    private int threads;
    protected int retryTimes = 5;
    protected String savePath;
    protected boolean saveTotal;
    protected String saveFormat;
    protected String saveSeparator;
    protected List<String> rmFields;
    protected List<String> fields;
    private ILineProcess<T> processor; // 定义的资源处理器
    ConcurrentMap<String, IResultOutput<W>> saverMap = new ConcurrentHashMap<>();
    ConcurrentMap<String, ILineProcess<T>> processorMap = new ConcurrentHashMap<>();

    public FileContainer(String filePath, String parse, String separator, String addKeyPrefix, String rmKeyPrefix,
                         Map<String, String> linesMap, Map<String, String> indexMap, List<String> fields, int unitLen,
                         int threads) {
        this.filePath = filePath;
        this.parse = parse;
        this.separator = separator;
        this.addKeyPrefix = addKeyPrefix;
        this.rmKeyPrefix = rmKeyPrefix;
        this.linesMap = linesMap == null ? new HashMap<>() : linesMap;
        this.indexMap = indexMap;
        this.unitLen = unitLen;
        this.threads = threads;
        // default save parameters
        this.saveTotal = false; // 默认全记录不保存
        this.savePath = "result";
        this.saveFormat = "tab";
        this.saveSeparator = "\t";
        if (fields == null || fields.size() == 0) {
            this.fields = ConvertingUtils.getOrderedFields(new ArrayList<>(this.indexMap.values()), rmFields);
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
            this.fields = ConvertingUtils.getFields(new ArrayList<>(fields), rmFields);
        }
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public void setProcessor(ILineProcess<T> processor) {
        this.processor = processor;
    }

    protected abstract ITypeConvert<String, T> getNewConverter() throws IOException;

    protected abstract ITypeConvert<T, String> getNewStringConverter() throws IOException;

    private JsonRecorder recorder = new JsonRecorder();

    public void export(IReader<E> reader, IResultOutput<W> saver, ILineProcess<T> processor) throws Exception {
        ITypeConvert<String, T> converter = getNewConverter();
        ITypeConvert<T, String> stringConverter = null;
        if (saveTotal) {
            stringConverter = getNewStringConverter();
            saver.preAddWriter("failed");
        }
        String lastLine = reader.lastLine();
        List<String> srcList = null;
        List<T> convertedList;
        List<String> writeList;
        int retry;
        while (lastLine != null) {
            if (LocalDateTime.now(clock).isAfter(pauseDateTime)) {
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
            if (processor != null) {
                lineProcessor = processor.clone();
                processorMap.put(orderStr, lineProcessor);
            }
            saver = getNewResultSaver(orderStr);
            saverMap.put(orderStr, saver);
            export(reader, saver, lineProcessor);
            recorder.remove(reader.getName());
            saverMap.remove(orderStr);
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
            if (saver != null) saver.closeWriters();
            if (lineProcessor != null) lineProcessor.closeResource();
            UniOrderUtils.returnOrder(order);
            reader.close();
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
            FileSaveMapper.ext = ".json";
            FileSaveMapper.append = false;
            String path = new File(savePath).getCanonicalPath();
            FileSaveMapper saveMapper = new FileSaveMapper(new File(path).getParent());
            String fileName = path.substring(path.lastIndexOf(FileUtils.pathSeparator) + 1) + "-lines";
            saveMapper.addWriter(fileName);
            saveMapper.writeToKey(fileName, record, true);
            saveMapper.closeWriters();
            rootLogger.info("please check the lines breakpoint in {}{}, it can be used for one more time reading remained lines.",
                    fileName, FileSaveMapper.ext);
        }
        procedureLogger.info(record);
    }

    private void showdownHook() {
        SignalHandler handler = signal -> {
            try {
                endAction();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.exit(0);
        };
        // 设置INT信号(Ctrl+C中断执行)交给指定的信号处理器处理，废掉系统自带的功能
        Signal.handle(new Signal("INT"), handler);
    }

    protected abstract List<IReader<E>> getFileReaders(String path) throws IOException;

    public void export() throws Exception {
        List<IReader<E>> fileReaders = getFileReaders(filePath);
        int filesCount = fileReaders.size();
        int runningThreads = filesCount < threads ? filesCount : threads;
        String info = "read objects from file(s): " + filePath + (processor == null ? "" : " and " + processor.getProcessName());
        rootLogger.info("{} running...", info);
        rootLogger.info("order\tpath\tquantity");
        ExecutorService executorPool = Executors.newFixedThreadPool(runningThreads);
        showdownHook();
        try {
            for (IReader<E> fileReader : fileReaders) {
                recorder.put(fileReader.getName(), "");
                executorPool.execute(() -> reading(fileReader));
            }
            executorPool.shutdown();
            while (!executorPool.isTerminated()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                    int i = 0;
                    while (i < 1000) i++;
                }
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

    private final Object object = new Object();
    private LocalDateTime pauseDateTime = LocalDateTime.now();
    private Clock clock = Clock.systemDefaultZone();

    public void export(LocalDateTime startTime, long pauseDelay, long duration) throws Exception {
        if (startTime != null) {
            Clock clock = Clock.systemDefaultZone();
            LocalDateTime now = LocalDateTime.now(clock);
            while (now.isBefore(startTime)) {
                System.out.printf("\r%s", LocalDateTime.now(clock).toString().substring(0, 19));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
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
