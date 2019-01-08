package com.qiniu.service.media;

import com.google.gson.JsonParser;
import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class QueryPfopResult implements ILineProcess<Map<String, String>>, Cloneable {

    private String persistentIdIndex;
    private MediaManager mediaManager;
    private String processName;
    private int retryCount;
    private String resultPath;
    private String resultTag;
    private int resultIndex;
    private FileMap fileMap;

    public QueryPfopResult(String persistentIdIndex, String resultPath, int resultIndex) throws IOException {
        this.processName = "pfopresult";
        if (persistentIdIndex == null || "".equals(persistentIdIndex))
            throw new IOException("please set the persistentIdIndex.");
        else this.persistentIdIndex = persistentIdIndex;
        this.mediaManager = new MediaManager();
        this.resultPath = resultPath;
        this.resultTag = "";
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap(resultPath, processName, String.valueOf(resultIndex));
        this.fileMap.initDefaultWriters();
    }

    public QueryPfopResult(String persistentIdIndex, String resultPath) throws IOException {
        this(persistentIdIndex, resultPath, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public void setResultTag(String resultTag) {
        this.resultTag = resultTag == null ? "" : resultTag;
    }

    public QueryPfopResult clone() throws CloneNotSupportedException {
        QueryPfopResult queryPfopResult = (QueryPfopResult)super.clone();
        queryPfopResult.mediaManager = new MediaManager();
        queryPfopResult.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(++resultIndex));
        try {
            queryPfopResult.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryPfopResult;
    }

    public String singleWithRetry(String id, int retryCount) throws QiniuException {
        String pfopResult = null;
        try {
            pfopResult = mediaManager.getPfopResultBodyById(id);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    pfopResult = mediaManager.getPfopResultBodyById(id);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }
        return pfopResult;
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        String pid;
        String pfopResult;
        JsonParser jsonParser = new JsonParser();
        for (Map<String, String> line : lineList) {
            pid = line.get(persistentIdIndex);
            try {
                pfopResult = singleWithRetry(pid, retryCount);
                if (pfopResult != null && !"".equals(pfopResult))
                    fileMap.writeSuccess(pid + "\t" + jsonParser.parse(pfopResult).toString());
                else
                    fileMap.writeError( pid + "\t" + String.valueOf(line) + "\tempty pfop result");
            } catch (QiniuException e) {
                String finalPid = pid;
                HttpResponseUtils.processException(e, fileMap, new ArrayList<String>(){{
                    add(finalPid + "\t" + String.valueOf(line));
                }});
            }
        }
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
