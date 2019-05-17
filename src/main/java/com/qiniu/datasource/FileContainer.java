package com.qiniu.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.util.FileNameUtils;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.SystemUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class FileContainer<E, W, T> implements IDataSource<IReader<E>, IResultOutput<W>, T> {

    private String filePath;
    protected String parseFormat;
    protected String separator;
    protected String addKeyPrefix;
    protected String rmKeyPrefix;
    protected Map<String, String> indexMap;
    protected int unitLen;
    private int threads;
    protected int retryTimes = 5;
    protected String savePath;
    protected boolean saveTotal;
    protected String saveFormat;
    protected String saveSeparator;
    protected List<String> rmFields;
    private ExecutorService executorPool; // 线程池
    private AtomicBoolean exitBool; // 多线程的原子操作 bool 值
    private ILineProcess<T> processor; // 定义的资源处理器

    public FileContainer(String filePath, String parseFormat, String separator, String addKeyPrefix, String rmKeyPrefix,
                         Map<String, String> indexMap, int unitLen, int threads) {
        this.filePath = filePath;
        this.parseFormat = parseFormat;
        this.separator = separator;
        this.addKeyPrefix = addKeyPrefix;
        this.rmKeyPrefix = rmKeyPrefix;
        this.indexMap = indexMap;
        this.unitLen = unitLen;
        this.threads = threads;
        this.saveTotal = false; // 默认全记录不保存
    }

    // 不调用则各参数使用默认值
    public void setSaveOptions(String savePath, boolean saveTotal, String format, String separator, List<String> rmFields) {
        this.savePath = savePath;
        this.saveTotal = saveTotal;
        this.saveFormat = format;
        this.saveSeparator = separator;
        this.rmFields = rmFields;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    // 通过 commonParams 来更新基本参数
    public void updateSettings(CommonParams commonParams) {
        this.filePath = commonParams.getPath();
        this.parseFormat = commonParams.getParse();
        this.separator = commonParams.getSeparator();
        this.addKeyPrefix = commonParams.getAddKeyPrefix();
        this.rmKeyPrefix = commonParams.getRmKeyPrefix();
        this.indexMap = commonParams.getIndexMap();
        this.unitLen = commonParams.getUnitLen();
        this.threads = commonParams.getThreads();
        this.retryTimes = commonParams.getRetryTimes();
        this.savePath = commonParams.getSavePath();
        this.saveTotal = commonParams.getSaveTotal();
        this.saveFormat = commonParams.getSaveFormat();
        this.saveSeparator = commonParams.getSaveSeparator();
        this.rmFields = commonParams.getRmFields();
    }

    public void setProcessor(ILineProcess<T> processor) {
        this.processor = processor;
    }

    protected abstract ITypeConvert<String, T> getNewConverter() throws IOException;

    protected abstract ITypeConvert<T, String> getNewStringConverter() throws IOException;

    public void export(IReader<E> reader, IResultOutput<W> saver, ILineProcess<T> processor) throws IOException {
        ITypeConvert<String, T> converter = getNewConverter();
        ITypeConvert<T, String> writeTypeConverter = getNewStringConverter();
        List<String> srcList = new ArrayList<>();
        List<T> convertedList;
        List<String> writeList;
        String line = "";
        int retry;
        while (line != null) {
            retry = retryTimes + 1;
            while (retry > 0) {
                try {
                    // 避免文件过大，行数过多，使用 lines() 的 stream 方式直接转换可能会导致内存泄漏，故使用 readLine() 的方式
                    line = reader.readLine();
                    retry = 0;
                } catch (IOException e) {
                    retry--;
                    if (retry == 0) throw e;
                }
            }
            if (line != null && !"".equals(line)) srcList.add(line);
            if (srcList.size() >= unitLen || (line == null && srcList.size() > 0)) {
                convertedList = converter.convertToVList(srcList);
                if (converter.errorSize() > 0)
                    saver.writeError(String.join("\n", converter.consumeErrors()), false);
                if (saveTotal) {
                    writeList = writeTypeConverter.convertToVList(convertedList);
                    if (writeList.size() > 0) saver.writeSuccess(String.join("\n", writeList), false);
                    if (writeTypeConverter.errorSize() > 0)
                        saver.writeError(String.join("\n", writeTypeConverter.consumeErrors()), false);
                }
                // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
                try {
                    if (processor != null) processor.processLine(convertedList);
                } catch (QiniuException e) {
                    // 这里其实逻辑上没有做重试次数的限制，因为返回的 retry 始终大于等于 -1，所以不是必须抛出的异常则会跳过，process 本身会
                    // 保存失败的记录，除非是 process 出现 599 状态码才会抛出异常
                    retry = HttpResponseUtils.checkException(e, 1);
                    if (retry == -2) throw e;
                }
                srcList.clear();
            }
        }
    }

    protected abstract IResultOutput<W> getNewResultSaver(String order) throws IOException;

    public void execInThread(IReader<E> reader, int order) throws Exception {
        // 如果是第一个线程直接使用初始的 processor 对象，否则使用 clone 的 processor 对象，多线程情况下不要直接使用传入的 processor，
        // 因为对其关闭会造成 clone 的对象无法进行结果持久化的写入
        ILineProcess<T> lineProcessor = processor == null ? null : processor.clone();
        // 持久化结果标识信息
        String newOrder = String.valueOf(order);
        IResultOutput<W> saver = getNewResultSaver(newOrder);
        executorPool.execute(() -> {
            try {
                String record = "order " + newOrder + ": " + reader.getName();
                export(reader, saver, lineProcessor);
                record += "\tsuccessfully done";
                System.out.println(record);
                saver.closeWriters();
                if (lineProcessor != null) lineProcessor.closeResource();
                reader.close();
            } catch (Exception e) {
                try {
                    System.out.println("order " + newOrder + ": " + reader.getName() + "\tnextLine:" + reader.readLine());
                } catch (IOException io) {
                    io.printStackTrace();
                }
                saver.closeWriters();
                if (lineProcessor != null) lineProcessor.closeResource();
                SystemUtils.exit(exitBool, e);
            }
        });
    }

    protected abstract IReader<E> getReader(String path) throws IOException;

    public void export() throws Exception {
        List<IReader<E>> fileReaders = new ArrayList<>();
        File sourceFile = new File(FileNameUtils.realPathWithUserHome(filePath));
        if (sourceFile.isDirectory()) {
            File[] fs = sourceFile.listFiles();
            if (fs == null) throw new IOException("The current path you gave may be incorrect: \"" + filePath + "\"");
            for(File f : fs) {
                if (!f.isDirectory() && f.getName().endsWith(".txt")) {
                    fileReaders.add(getReader(f.getAbsoluteFile().getPath()));
                }
            }
        } else {
            if (filePath.endsWith(".txt")) {
                fileReaders.add(getReader(filePath));
            } else {
                throw new IOException("please provide the .txt file. The current path you gave is: \"" + filePath + "\"");
            }
        }
        if (fileReaders.size() == 0) throw new IOException("please provide the .txt file int the directory. The current" +
                " path you gave is: " + filePath);

        int filesCount = fileReaders.size();
        int runningThreads = filesCount < threads ? filesCount : threads;
        String info = "read objects from file(s): " + filePath + (processor == null ? "" : " and " + processor.getProcessName());
        System.out.println(info + " running...");
        executorPool = Executors.newFixedThreadPool(runningThreads);
        exitBool = new AtomicBoolean(false);
        try {
            int order = 1;
            for (IReader<E> fileReader : fileReaders) {
                execInThread(fileReader, order++);
            }
            executorPool.shutdown();
            while (!executorPool.isTerminated()) Thread.sleep(1000);
            System.out.println(info + " finished");
        } catch (Throwable throwable) {
            SystemUtils.exit(exitBool, throwable);
        }
    }
}
