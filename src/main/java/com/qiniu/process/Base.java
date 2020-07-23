package com.qiniu.process;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IFileChecker;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.util.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * process 基类，实现 ILineProcess 的方法，并实现 Cloneable 接口支持 clone。该类为抽象类，且子类需要实现一些抽象方法（子类实现抽象方法时不需
 * 要同步代码），包内提供的子类均为非线程安全，且都调用该基类的 close 方法，多线程时需使用 clone 的对象，否则容易引发线程安全问题，同时，即使子类
 * 使用同步方法，多线程时调用了一次 close 会导致其他线程跑异常。
 * @param <T> line 数据范型
 */
public abstract class Base<T> implements ILineProcess<T>, Cloneable {

    protected String processName;
    protected String accessId;
    protected String secretKey;
    protected String bucket;
    protected int batchSize;
    protected int retryTimes = 5;
    protected boolean autoIncrease;
    protected int index;
    protected AtomicInteger saveIndex;
    protected String savePath;
    protected FileSaveMapper fileSaveMapper;
    protected IFileChecker iFileChecker;
    protected String checkType;
    protected boolean canceled;

    public Base(String processName, String accessId, String secretKey, String bucket) {
        this.processName = processName;
        this.accessId = accessId;
        this.secretKey = secretKey;
        this.bucket = bucket;
        this.iFileChecker = key -> null;
    }

    public Base(String processName, String accessKey, String secretKey, String bucket, String savePath, int saveIndex)
            throws IOException {
        this(processName, accessKey, secretKey, bucket);
        this.index = saveIndex;
        this.saveIndex = new AtomicInteger(saveIndex);
        this.savePath = savePath;
        this.fileSaveMapper = new FileSaveMapper(savePath, processName, String.valueOf(saveIndex));
        this.fileSaveMapper.preAddWriter("need_retry");
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setAutoIncrease(boolean autoIncrease) {
        this.autoIncrease = autoIncrease;
    }

    public void setBatchSize(int batchSize) throws IOException {
        if (!ProcessUtils.canBatch(processName)) {
            throw new IOException(processName + " is not support batch operation.");
        } else if (batchSize > 1000 || batchSize < 0) {
            throw new IOException("batch size must less than 1000 and more than 0.");
        } else {
            this.batchSize = batchSize;
            batchSizeTrigger();
        }
    }

    public void batchSizeTrigger() {}

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes < 1 ? 5 : retryTimes;
    }

    public void updateFields(Map<String, String> fieldsMap) {
        Class cClass = this.getClass();
        fieldsMap.forEach((key, value) -> {
            try {
                Field field = cClass.getDeclaredField(key);
                field.setAccessible(true);
                field.set(this, value);
            } catch (Exception ignored) {
                ignored.printStackTrace();
            }
        });
    }

    @SuppressWarnings("unchecked")
    public Base<T> clone() throws CloneNotSupportedException {
        Base<T> base = (Base<T>)super.clone();
        if (checkType != null && !"".equals(checkType)) base.iFileChecker = fileCheckerInstance();
        if (fileSaveMapper == null) return base;
        try {
            base.fileSaveMapper = autoIncrease ?
                    new FileSaveMapper(savePath, processName, String.valueOf(saveIndex.addAndGet(1))) :
                    new FileSaveMapper(savePath);
            base.fileSaveMapper.preAddWriter("need_retry");
        } catch (IOException e) {
            throw new CloneNotSupportedException(e.getMessage() + ", init writer failed.");
        }
        return base;
    }

    public void changeSaveOrder(String order) throws IOException {
        try {
            this.fileSaveMapper.changePrefixAndSuffix(processName, order);
        } catch (NullPointerException e) {
            throw new IOException("instance without savePath can not call changeSaveOrder method.");
        }
    }

    protected IFileChecker fileCheckerInstance() {
        return key -> null;
    }

    public void setCheckType(String checkType) {
        this.checkType = checkType;
        this.iFileChecker = fileCheckerInstance();
    }

    /**
     * 当处理结束应该从 line 中记录哪些关键信息
     * @param line 输入的 line
     * @return 返回需要记录的信息字符串
     */
    protected abstract String resultInfo(T line);

    protected List<T> putBatchOperations(List<T> processList) throws IOException {
        return processList;
    }

    /**
     * 对 lineList 执行 batch 的操作，因为默认是实现单个资源请求的操作，部分操作不支持 batch，因此需要 batch 操作时子类需要重写该方法。
     * @param lineList 代执行的文件信息列表
     * @return 返回执行响应信息的字符串
     * @throws Exception 执行失败抛出的异常
     */
    protected String batchResult(List<T> lineList) throws Exception {
        throw new IOException("no default batch operation, please implements batch processing by yourself.");
    }

    /**
     * 处理 batchOperations 执行的结果，将输入的文件信息和结果对应地记录下来
     * @param processList batch 操作的资源列表
     * @param result batch 操作之后的响应结果
     * @return 返回需要进行重试的记录列表
     * @throws Exception 处理结果失败抛出的异常
     */
    protected List<T> parseBatchResult(List<T> processList, String result) throws Exception {
        if (result == null || "".equals(result)) throw new IOException("not valid json.");
        List<T> retryList = null;
        JsonArray jsonArray = JsonUtils.fromJson(result, JsonArray.class);
        // 正常情况下 jsonArray 和 processList 的长度是相同的，将输入行信息和执行结果一一对应记录
        if (processList.size() != jsonArray.size()) throw new IOException("not matched process and return size.");
        JsonObject jsonObject;
        for (int j = 0; j < processList.size(); j++) {
            jsonObject = jsonArray.get(j).getAsJsonObject();
            switch (HttpRespUtils.checkStatusCode(jsonObject.get("code").getAsInt())) {
                case 1:
                    fileSaveMapper.writeSuccess(String.join("\t", resultInfo(processList.get(j)),
                            jsonObject.toString()), false);
                    break;
                case 0:
                    if (retryList == null) retryList = new ArrayList<>(1);
                    retryList.add(processList.get(j)); // 放回重试列表
                    break;
                case -1:
                    fileSaveMapper.writeError(String.join("\t", resultInfo(processList.get(j)),
                            jsonObject.toString()), false);
                    break;
            }
        }
        return retryList;
    }

    /**
     * 批量处理输入行，具体执行的操作取决于 batchResult 方法的实现。
     * @param lineList 输入列表
     * @param batchSize 一次批量处理请求处理的文件个数
     * @param retryTimes 每一行信息处理时需要的重试次数
     * @throws IOException 处理失败可能抛出的异常
     */
    private void batchProcess(List<T> lineList, int batchSize, int retryTimes) throws IOException {
        int times = (lineList.size() + batchSize - 1) / batchSize;
        List<T> processList;
        int retry;
        String message = null;
        QiniuException exception = null;
        for (int i = 0; i < times; i++) {
            if (canceled) break;
            retry = retryTimes;
            processList = lineList.subList(batchSize * i, i == times - 1 ? lineList.size() : batchSize * (i + 1));
            // 加上 processList.size() > 0 的选择原因是会在每一次处理 batch 操作的结果时将需要重试的记录加入重试列表进行返回，并且在
            // 没有异常的情况下当前的 processList 会执行到没有重试记录返回时才结束，parseBatchResult 会返回可以重试的列表，无重试记录则返回
            // 空，重试次数小于 0 时设置 processList = null
            while (processList != null && processList.size() > 0) {
                try {
                    processList = putBatchOperations(processList);
                    if (processList.size() > 0) {
                        processList = parseBatchResult(processList, batchResult(processList));
                    }
                } catch (QiniuException e) {
                    retry = HttpRespUtils.checkException(e, retry);
                    message = HttpRespUtils.getMessage(e);
                    exception = e;
                } catch (Exception e) {
                    retry = 0;
                    message = e.getMessage();
                    exception = null;
                }
                switch (retry) {
                    case 0: fileSaveMapper.writeError(String.join("\t", processList.stream()
                            .map(this::resultInfo).collect(Collectors.joining("\n")), message), false);
                        processList = null; break;
                    case -1: fileSaveMapper.writeToKey("need_retry", String.join("\t", processList
                            .stream().map(this::resultInfo).collect(Collectors.joining("\n")), message), false);
                        processList = null; break;
                    case -2: fileSaveMapper.writeError(String.join("\t", lineList
                            .subList(batchSize * i, lineList.size()).stream().map(this::resultInfo)
                            .collect(Collectors.joining("\n")), message), false);
                        if (exception != null) throw exception;
                }
                if (exception != null && exception.response != null) exception.response.close();
            }
        }
    }

    /**
     * 单个文件进行操作的方法，返回操作的结果字符串，要求子类必须实现该方法，支持单个资源依次请求操作
     * @param line 输入 line
     * @return 操作结果的字符串
     * @throws Exception 操作失败时的返回
     */
    abstract protected String singleResult(T line) throws Exception;

    /**
     * 处理 singleProcess 执行的结果，默认情况下直接使用 resultInfo 拼接 result 成一行执行持久化写入，部分 process 可能对结果做进一步判断
     * 需要重写该方法
     * @param line 输入的 map 数据
     * @param result singleResult 的结果字符串
     * @throws Exception 处理结果失败抛出异常
     */
    protected void parseSingleResult(T line, String result) throws Exception {
        fileSaveMapper.writeSuccess(result, false);
    }

    /**
     * 对输入的文件信息列表单个进行操作，具体的操作方法取决于 singleResult 方法
     * @param lineList 输入列表
     * @param retryTimes 每一行信息处理时需要的重试次数
     * @throws IOException 处理失败可能抛出的异常
     */
    private void singleProcess(List<T> lineList, int retryTimes) throws IOException {
        int retry;
        T line;
        String message;
        QiniuException exception;
        for (int i = 0; i < lineList.size(); i++) {
            line = lineList.get(i);
            retry = retryTimes;
            while (retry > 0) {
                try {
                    parseSingleResult(line, singleResult(line));
                    break;
                } catch (QiniuException e) {
                    retry = HttpRespUtils.checkException(e, retry);
                    message = HttpRespUtils.getMessage(e);
                    exception = e;
                } catch (Exception e) {
                    retry = 0;
                    message = e.getMessage();
                    exception = null;
                }
                switch (retry) {
                    case 0: fileSaveMapper.writeError(String.join("\t", resultInfo(line), message),
                            false); break;
                    case -1: fileSaveMapper.writeToKey("need_retry", String.join("\t", resultInfo(line),
                            message), false); break;
                    case -2: fileSaveMapper.writeError(String.join("\t", lineList.subList(i, lineList.size())
                            .stream().map(this::resultInfo).collect(Collectors.joining("\n")), message), false);
                        throw exception;
                }
                if (exception != null && exception.response != null) exception.response.close();
            }
        }
    }

    public String processLine(T line) throws IOException {
        try {
            return singleResult(line);
        } catch (NullPointerException e) {
            if (canceled) {
                throw new IOException("processor in canceled state.");
            } else if (batchSize < 0) { // 如果是关闭了那么 batchSize 应该小于 0
                throw new IOException("input is empty or the processor may be already closed.");
            } else {
                throw new IOException("instance without savePath can not call this batch process method.");
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * 公开的操作调用方法入口，通过判断 batch size 来决定调用哪个方法
     * @param lineList 输入的文件信息列表
     * @throws IOException 处理过程中出现的异常
     */
    public void processLine(List<T> lineList) throws IOException {
        try {
            if (batchSize > 1) batchProcess(lineList, batchSize, retryTimes);
            else singleProcess(lineList, retryTimes);
        } catch (NullPointerException e) {
            if (canceled) {
////            // nothing to do
            } else if (batchSize < 0) { // 如果是关闭了那么 batchSize 应该小于 0
                throw new IOException("input is empty or the processor may be already closed.");
            } else {
                throw new IOException("instance without savePath can not call this batch process method.");
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    public void closeResource() {
        accessId = null;
        secretKey = null;
        bucket = null;
        batchSize = -1;
        saveIndex = null;
        savePath = null;
        if (fileSaveMapper != null) fileSaveMapper.closeWriters();
        fileSaveMapper = null;
        iFileChecker = null;
    }

    public void cancel() {
        canceled = true;
        closeResource();
    }
}
