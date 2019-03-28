package com.qiniu.process;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.persistence.FileMap;
import com.qiniu.storage.Configuration;
import com.qiniu.util.FileNameUtils;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.LogUtils;
import com.qiniu.util.ProcessUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class Base implements ILineProcess<Map<String, String>>, Cloneable {

    final protected String processName;
    protected Configuration configuration;
    protected String accessKey;
    protected String secretKey;
    protected String bucket;
    protected String rmPrefix;
    protected int batchSize;
    protected int retryTimes = 5;
    protected int saveIndex;
    protected String savePath;
    protected FileMap fileMap;

    public Base(String processName, String accessKey, String secretKey, Configuration configuration,
                String bucket, String rmPrefix, String savePath, int saveIndex) throws IOException {
        this.processName = processName;
        this.configuration = configuration;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket = bucket;
        this.rmPrefix = rmPrefix;
        this.saveIndex = saveIndex;
        this.savePath = savePath;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
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
        this.fileMap.closeWriters();
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public Base clone() throws CloneNotSupportedException {
        Base base = (Base)super.clone();
        base.fileMap = new FileMap(savePath, processName, String.valueOf(++saveIndex));
        try {
            base.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return base;
    }

    protected Map<String, String> formatLine(Map<String, String> line) throws IOException {
        line.put("key", FileNameUtils.rmPrefix(rmPrefix, line.get("key")));
        return line;
    }

    /**
     * 当处理失败的时候应该从 line 中记录哪些关键信息，默认只记录 key，需要子类去重写记录更多信息
     * @param line 输入的 line
     * @return 返回需要记录的信息字符串
     */
    protected String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    /**
     * 对 lineList 执行 batch 的操作，因为默认是实现单个资源请求的操作，部分操作不支持 batch，因此需要 batch 操作时子类需要重写该方法。
     * @param lineList 代执行的文件信息列表
     * @return 返回执行响应信息的字符串
     * @throws QiniuException 执行失败抛出的异常
     */
    protected String batchResult(List<Map<String, String>> lineList) throws QiniuException {
        return null;
    }

    /**
     * 处理 batchOperations 执行的结果，将输入的文件信息和结果对应地记录下来
     * @param processList batch 操作的资源列表
     * @param result batch 操作之后的响应结果
     * @throws IOException 写入结果失败抛出的异常
     */
    protected void parseBatchResult(List<Map<String, String>> processList, String result) throws IOException {
        if (result == null || "".equals(result)) throw new IOException("not valid json.");
        JsonArray jsonArray;
        try {
            jsonArray = new Gson().fromJson(result, JsonArray.class);
        } catch (JsonParseException e) {
            throw new IOException("parse to json array error.");
        }
        JsonObject jsonObject;
        for (int j = 0; j < processList.size(); j++) {
            jsonObject = jsonArray.get(j).getAsJsonObject();
            // 正常情况下 jsonArray 和 processList 的长度是相同的，将输入行信息和执行结果一一对应记录，否则结果记录为空
            if (j < jsonArray.size()) {
                if (jsonObject.get("code").getAsInt() == 200)
                    fileMap.writeSuccess(resultInfo(processList.get(j)) + "\t" + jsonObject, false);
                else
                    fileMap.writeError(resultInfo(processList.get(j)) + "\t" + jsonObject, false);
            } else {
                fileMap.writeError(resultInfo(processList.get(j)) + "empty_result", false);
            }
        }
    }

    /**
     * 批量处理输入行，具体执行的操作取决于 batchResult 方法的实现
     * @param lineList 输入列表
     * @param retryTimes 每一行信息处理时需要的重试次数
     * @throws IOException 处理失败可能抛出的异常
     */
    protected void batchProcess(List<Map<String, String>> lineList, int retryTimes) throws IOException {
        // 先进行过滤修改
        List<String> errorLineList = new ArrayList<>();
        lineList = lineList.stream().filter(line -> {
            try {
                line = formatLine(line);
                return true;
            } catch (IOException e) {
                errorLineList.add(resultInfo(line) + "\t" + e.getMessage());
                return false;
            }
        }).collect(Collectors.toList());
        if (errorLineList.size() > 0) fileMap.writeError(String.join("\n", errorLineList), false);
        int times = lineList.size()/batchSize + 1;
        List<Map<String, String>> processList;
        String result;
        int retry;
        for (int i = 0; i < times; i++) {
            processList = lineList.subList(batchSize * i, i == times - 1 ? lineList.size() : batchSize * (i + 1));
            if (processList.size() > 0) {
                retry = retryTimes + 1; // 不执行重试的话本身需要一次执行机会
                while (retry > 0) {
                    try {
                        result = batchResult(processList);
                        parseBatchResult(processList, result);
                        retry = 0;
                    } catch (QiniuException e) {
                        retry = HttpResponseUtils.checkException(e, retry);
                        String message = LogUtils.getMessage(e).replaceAll("\n", "\t");
                        System.out.println(message);
                        if (retry <= 0) {
                            fileMap.writeError(String.join("\n", lineList.subList(i, lineList.size() - 1)
                                    .stream().map(line -> line + "\t" + message.replaceAll("\n", "\t"))
                                    .collect(Collectors.toList())), false);
                        }
                        if (retry == -1) throw e;
                    }
                }
            }
        }
    }

    /**
     * 单个文件进行操作的方法，返回操作的结果字符串，要求子类必须实现该方法，支持单个资源依次请求操作
     * @param line 输入 line
     * @return 操作结果的字符串
     * @throws QiniuException 操作失败时的返回
     */
    abstract protected String singleResult(Map<String, String> line) throws QiniuException;

    /**
     * 对输入的文件信息列表单个进行操作，具体的操作方法取决于 singleResult 方法
     * @param lineList 输入列表
     * @param retryTimes 每一行信息处理时需要的重试次数
     * @throws IOException 处理失败可能抛出的异常
     */
    protected void singleProcess(List<Map<String, String>> lineList, int retryTimes) throws IOException {
        String result;
        int retry;
        Map<String, String> line;
        for (int i = 0; i < lineList.size(); i++) {
            line = lineList.get(i);
            try {
                line = formatLine(line);
            } catch (IOException e) {
                fileMap.writeError(resultInfo(line) + "\t" + e.getMessage(), false);
                continue;
            }
            retry = retryTimes + 1; // 不执行重试的话本身需要一次执行机会
            while (retry > 0) {
                try {
                    result = singleResult(line);
                    fileMap.writeSuccess(result, false);
                    retry = 0;
                } catch (QiniuException e) {
                    retry = HttpResponseUtils.checkException(e, retry);
                    String message = LogUtils.getMessage(e).replaceAll("\n", "\t");
                    System.out.println(message);
                    if (retry == 0) {
                        fileMap.writeError(resultInfo(line) + "\t" + message, false);
                    } else if (retry == -1) {
                        fileMap.writeError(String.join("\n", lineList.subList(i, lineList.size() - 1).stream()
                                .map(srcLine -> resultInfo(srcLine) + "\t" + message)
                                .collect(Collectors.toList())), false);
                        throw e;
                    }
                }
            }
        }
    }

    /**
     * 公开的操作调用方入口，通过判断 batch size 来决定调用哪个方法
     * @param lineList 输入的文件信息列表
     * @throws IOException 处理过程中出现的异常
     */
    public void processLine(List<Map<String, String>> lineList) throws IOException {
        if (batchSize > 1) batchProcess(lineList, retryTimes);
        else singleProcess(lineList, retryTimes);
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
