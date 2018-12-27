package com.qiniu.service.media;

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
    private int resultIndex;
    private FileMap fileMap;

    public QueryPfopResult(String persistentIdIndex, String resultPath, int resultIndex) throws IOException {
        this.processName = "pfopresult";
        if (persistentIdIndex == null || "".equals(persistentIdIndex))
            throw new IOException("please set the persistentIdIndex.");
        else this.persistentIdIndex = persistentIdIndex;
        this.mediaManager = new MediaManager();
        this.resultPath = resultPath;
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap();
        this.fileMap.initWriter(resultPath, processName, resultIndex);
    }

    public QueryPfopResult(String persistentIdIndex, String resultPath) throws IOException {
        this(persistentIdIndex, resultPath, 0);
    }

    public QueryPfopResult clone() throws CloneNotSupportedException {
        QueryPfopResult queryPfopResult = (QueryPfopResult)super.clone();
        queryPfopResult.mediaManager = new MediaManager();
        queryPfopResult.fileMap = new FileMap();
        try {
            queryPfopResult.fileMap.initWriter(resultPath, processName, resultIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryPfopResult;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getProcessName() {
        return this.processName;
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

    public void processLine(List<Map<String, String>> lineList) throws QiniuException {
        List<String> resultList = new ArrayList<>();
        for (Map<String, String> line : lineList) {
            try {
                String pfopResult = singleWithRetry(line.get(persistentIdIndex), retryCount);
                if (pfopResult != null)resultList.add(pfopResult);
                else throw new QiniuException(null, "empty pfop result");
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, line.toString());
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
