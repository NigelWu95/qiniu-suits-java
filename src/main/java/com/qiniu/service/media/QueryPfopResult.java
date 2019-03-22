package com.qiniu.service.media;

import com.google.gson.Gson;
import com.qiniu.model.media.Item;
import com.qiniu.model.media.PfopResult;
import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.LogUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryPfopResult implements ILineProcess<Map<String, String>>, Cloneable {

    final private String processName;
    final private String pidIndex;
    private MediaManager mediaManager;
    private int retryTimes = 3;
    final private String savePath;
    private String saveTag;
    private int saveIndex;
    private FileMap fileMap;

    public QueryPfopResult(String pidIndex, String savePath, int saveIndex) throws IOException {
        this.processName = "pfopresult";
        if (pidIndex == null || "".equals(pidIndex)) throw new IOException("please set the persistentIdIndex.");
        else this.pidIndex = pidIndex;
        this.mediaManager = new MediaManager();
        this.savePath = savePath;
        this.saveTag = "";
        this.saveIndex = saveIndex;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public QueryPfopResult(String persistentIdIndex, String savePath) throws IOException {
        this(persistentIdIndex, savePath, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes < 1 ? 3 : retryTimes;
    }

    public void setSaveTag(String saveTag) {
        this.saveTag = saveTag == null ? "" : saveTag;
    }

    public QueryPfopResult clone() throws CloneNotSupportedException {
        QueryPfopResult queryPfopResult = (QueryPfopResult)super.clone();
        queryPfopResult.mediaManager = new MediaManager();
        queryPfopResult.fileMap = new FileMap(savePath, processName, saveTag + String.valueOf(++saveIndex));
        try {
            queryPfopResult.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryPfopResult;
    }

    /**
     * 处理操作的结果
     * @param line 原始的 line
     * @param result 处理的结果字符串
     * @throws IOException 写入处理结果报错
     */
    private void parseResult(Map<String, String> line, String result) throws IOException{
        if (result != null && !"".equals(result)) {
            PfopResult pfopResult = new Gson().fromJson(result, PfopResult.class);
            // 可能有多条转码指令
            for (Item item : pfopResult.items) {
                // code == 0 时表示转码已经成功，不成功的情况下记录下转码参数和错误方便进行重试
                if (item.code == 0) {
                    fileMap.writeSuccess(line.get(pidIndex) + "\t" + pfopResult.inputKey + "\t" +
                            item.key + "\t" + result, false);
                } else {
                    fileMap.writeError( line.get(pidIndex) + "\t" + pfopResult.inputKey + "\t" +
                            item.key + "\t" + item.cmd + "\t" + item.code + "\t" + item.desc + "\t" +
                            item.error, false);
                }
            }
        } else {
            fileMap.writeKeyFile("empty_result", line.get(pidIndex), false);
        }
    }

    /**
     * 批量处理输入行进行 pfop result 的查询
     * @param lineList 输入列表
     * @param retryTimes 每一行信息处理时需要的重试次数
     * @throws IOException 处理失败可能抛出的异常
     */
    public void processLine(List<Map<String, String>> lineList, int retryTimes) throws IOException {
        String result;
        int retry;
        Map<String, String> line;
        for (int i = 0; i < lineList.size(); i++) {
            line = lineList.get(i);
            retry = retryTimes;
            while (retry > 0) {
                try {
                    result = mediaManager.getPfopResultBodyById(line.get(pidIndex));
                    parseResult(line, result);
                    retry = 0;
                } catch (QiniuException e) {
                    retry = HttpResponseUtils.checkException(e, retry);
                    if (retry == 0) LogUtils.writeLog(e, fileMap, line.get("key"));
                    else if (retry == -1) {
                        LogUtils.writeLog(e, fileMap, lineList.subList(i, lineList.size() - 1).parallelStream()
                                .map(String::valueOf).collect(Collectors.toList()));
                        throw e;
                    }
                }
            }
        }
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        processLine(lineList, retryTimes);
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
