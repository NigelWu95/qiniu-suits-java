package com.qiniu.process;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class Base<T> implements ILineProcess<T>, Cloneable {

    private String processName;
    protected Configuration configuration;
    protected String accessKey;
    protected String secretKey;
    protected String bucket;
    protected int batchSize;
    protected int retryTimes = 5;
    protected int saveIndex;
    protected String savePath;
    protected FileSaveMapper fileSaveMapper;

    public Base(String processName, String accessKey, String secretKey, Configuration configuration, String bucket,
                String savePath, int saveIndex) throws IOException {
        if (ProcessUtils.needConfiguration(processName) && configuration == null)
            throw new IOException("please set configuration, it can not be null.");
        this.processName = processName;
        this.configuration = configuration;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket = bucket;
        this.saveIndex = saveIndex;
        this.savePath = savePath;
        this.fileSaveMapper = new FileSaveMapper(savePath, processName, String.valueOf(saveIndex));
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setBatchSize(int batchSize) throws IOException {
        if (!ProcessUtils.canBatch(processName)) {
            throw new IOException(processName + " is not support batch operation.");
        } else if (batchSize > 1000) {
            throw new IOException("batch size must less than 1000.");
        } else {
            this.batchSize = batchSize;
        }
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes < 1 ? 3 : retryTimes;
    }

    public void updateSavePath(String savePath) throws IOException {
        this.savePath = savePath;
        this.fileSaveMapper.closeWriters();
        this.fileSaveMapper = new FileSaveMapper(savePath, processName, String.valueOf(saveIndex));
    }

    @SuppressWarnings("unchecked")
    public Base<T> clone() throws CloneNotSupportedException {
        Base<T> base = (Base<T>)super.clone();
        try {
            base.fileSaveMapper = new FileSaveMapper(savePath, processName, String.valueOf(++saveIndex));
        } catch (IOException e) {
            throw new CloneNotSupportedException(e.getMessage() + ", init writer failed.");
        }
        return base;
    }

    /**
     * 当处理结束应该从 line 中记录哪些关键信息
     * @param line 输入的 line
     * @return 返回需要记录的信息字符串
     */
    public abstract String resultInfo(T line);

    public abstract boolean validCheck(T line);

    /**
     * 对 lineList 执行 batch 的操作，因为默认是实现单个资源请求的操作，部分操作不支持 batch，因此需要 batch 操作时子类需要重写该方法。
     * @param lineList 代执行的文件信息列表
     * @return 返回执行响应信息的字符串
     * @throws QiniuException 执行失败抛出的异常
     */
    public String batchResult(List<T> lineList) throws IOException {
        return null;
    }

    /**
     * 处理 batchOperations 执行的结果，将输入的文件信息和结果对应地记录下来
     * @param processList batch 操作的资源列表
     * @param result batch 操作之后的响应结果
     * @return 返回需要进行重试的记录列表
     * @throws IOException 写入结果失败抛出的异常
     */
    public List<T> parseBatchResult(List<T> processList, String result) throws IOException {
        if (result == null || "".equals(result)) throw new IOException("not valid json.");
        List<T> retryList = new ArrayList<>();
        JsonArray jsonArray;
        try {
            jsonArray = JsonConvertUtils.fromJson(result, JsonArray.class);
        } catch (JsonParseException e) {
            throw new IOException("parse to json array error.");
        }
        JsonObject jsonObject;
        for (int j = 0; j < processList.size(); j++) {
            jsonObject = jsonArray.get(j).getAsJsonObject();
            // 正常情况下 jsonArray 和 processList 的长度是相同的，将输入行信息和执行结果一一对应记录，否则结果记录为空
            if (j < jsonArray.size()) {
                switch (HttpResponseUtils.checkStatusCode(jsonObject.get("code").getAsInt())) {
                    case 1:
                        fileSaveMapper.writeSuccess(resultInfo(processList.get(j)) + "\t" + jsonObject, false);
                        break;
                    case 0:
                        retryList.add(processList.get(j)); // 放回重试列表
                        break;
                    case -1:
                        fileSaveMapper.writeError(resultInfo(processList.get(j)) + "\t" + jsonObject, false);
                        break;
                }
            } else {
                fileSaveMapper.writeError(resultInfo(processList.get(j)) + "empty_result", false);
            }
        }
        return retryList;
    }

    /**
     * 批量处理输入行，具体执行的操作取决于 batchResult 方法的实现。
     * @param lineList 输入列表
     * @param retryTimes 每一行信息处理时需要的重试次数
     * @throws IOException 处理失败可能抛出的异常
     */
    public void batchProcess(List<T> lineList, int retryTimes) throws IOException {
        // 先进行过滤修改
        List<String> errorLineList = new ArrayList<>();
        lineList = lineList.stream().filter(line -> {
            if (!validCheck(line)) {
                errorLineList.add(resultInfo(line) + "\tempty target key's value in line.");
                return false;
            } else {
                return true;
            }
        }).collect(Collectors.toList());
        if (errorLineList.size() > 0) fileSaveMapper.writeError(String.join("\n", errorLineList), false);
        int times = lineList.size()/batchSize + 1;
        List<T> processList;
        String result;
        int retry;
        for (int i = 0; i < times; i++) {
            processList = lineList.subList(batchSize * i, i == times - 1 ? lineList.size() : batchSize * (i + 1));
            if (processList.size() > 0) {
                retry = retryTimes + 1; // 不执行重试的话本身需要一次执行机会
                // 加上 processList.size() > 0 的选择原因是会在每一次处理 batch 操作的结果时将需要重试的记录加入重试列表进行返回，并且在
                // 没有异常的情况下当前的 processList 会执行到没有重试记录返回时才结束
                while (retry > 0 || processList.size() > 0) {
                    try {
                        result = batchResult(processList);
                        processList = parseBatchResult(processList, result);
                        retry = 0;
                    } catch (QiniuException e) {
                        retry = HttpResponseUtils.checkException(e, retry);
                        String message = LogUtils.getMessage(e);
                        System.out.println(message);
                        switch (retry) { // 实际上 batch 操作产生异常经过 checkException 不会出现返回 0 的情况
                            case 0: fileSaveMapper.writeError(String.join("\n", processList.stream()
                                    .map(this::resultInfo).collect(Collectors.toList())) + "\t" + message, false);
                            break;
                            case -1: fileSaveMapper.writeKeyFile("need_retry", String.join("\n", processList
                                    .stream().map(this::resultInfo).collect(Collectors.toList())) + "\t" + message, false);
                            break;
                            case -2: fileSaveMapper.writeError(String.join("\n", lineList
                                    .subList(batchSize * i, lineList.size()).stream()
                                    .map(this::resultInfo).collect(Collectors.toList())) + "\t" + message, false);
                            throw e;
                        }
                    }
                }
            }
        }
    }

    /**
     * 单个文件进行操作的方法，返回操作的结果字符串，要求子类必须实现该方法，支持单个资源依次请求操作
     * @param line 输入 line
     * @return 操作结果的字符串
     * @throws IOException 操作失败时的返回
     */
    abstract public String singleResult(T line) throws IOException;

    /**
     * 处理 singleProcess 执行的结果，默认情况下直接使用 resultInfo 拼接 result 成一行执行持久化写入，部分 process 可能对结果做进一步判断
     * 需要重写该方法
     * @param line 输入的 map 数据
     * @param result singleResult 的结果字符串
     * @throws IOException 写入结果失败抛出异常
     */
    public void parseSingleResult(T line, String result) throws IOException {
        fileSaveMapper.writeSuccess(resultInfo(line) + "\t" + result, false);
    }

    /**
     * 对输入的文件信息列表单个进行操作，具体的操作方法取决于 singleResult 方法
     * @param lineList 输入列表
     * @param retryTimes 每一行信息处理时需要的重试次数
     * @throws IOException 处理失败可能抛出的异常
     */
    public void singleProcess(List<T> lineList, int retryTimes) throws IOException {
        String result;
        int retry;
        T line;
        for (int i = 0; i < lineList.size(); i++) {
            line = lineList.get(i);
            if (!validCheck(line)) {
                fileSaveMapper.writeError(resultInfo(line) + "\tempty target key's value in line.", false);
                continue;
            }
            retry = retryTimes + 1; // 不执行重试的话本身需要一次执行机会
            while (retry > 0) {
                try {
                    result = singleResult(line);
                    parseSingleResult(line, result);
                    retry = 0;
                } catch (QiniuException e) {
                    retry = HttpResponseUtils.checkException(e, retry);
                    String message = LogUtils.getMessage(e);
                    System.out.println(message);
                    switch (retry) {
                        case 0: fileSaveMapper.writeError(resultInfo(line) + "\t" + message, false); break;
                        case -1: fileSaveMapper.writeKeyFile("need_retry", resultInfo(line) + "\t" + message, false);
                        break;
                        case -2: fileSaveMapper.writeError(String.join("\n", lineList.subList(i, lineList.size())
                                .stream().map(this::resultInfo).collect(Collectors.toList())) + "\t" + message, false);
                        throw e;
                    }
                }
            }
        }
    }

    public String processLine(T line) throws IOException {
        return singleResult(line);
    }

    /**
     * 公开的操作调用方法入口，通过判断 batch size 来决定调用哪个方法
     * @param lineList 输入的文件信息列表
     * @throws IOException 处理过程中出现的异常
     */
    public void processLine(List<T> lineList) throws IOException {
        if (batchSize > 1) batchProcess(lineList, retryTimes);
        else singleProcess(lineList, retryTimes);
    }

    public void closeResource() {
        fileSaveMapper.closeWriters();
    }
}
