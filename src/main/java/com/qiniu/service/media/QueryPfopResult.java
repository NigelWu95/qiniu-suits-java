package com.qiniu.service.media;

import com.qiniu.common.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.model.media.PfopResult;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class QueryPfopResult implements ILineProcess<Map<String, String>>, Cloneable {

    private MediaManager mediaManager;
    private String processName;
    private int retryCount = 3;
    protected String resultFileDir;
    private int resultFileIndex;
    private FileMap fileMap;

    private void initBaseParams() {
        this.processName = "fopresult";
    }

    public QueryPfopResult(String resultFileDir) {
        initBaseParams();
        this.resultFileDir = resultFileDir;
        this.mediaManager = new MediaManager();
        this.fileMap = new FileMap();
    }

    public QueryPfopResult(String resultFileDir, int resultFileIndex) throws IOException {
        this(resultFileDir);
        this.resultFileIndex = resultFileIndex;
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public QueryPfopResult getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        QueryPfopResult queryPfopResult = (QueryPfopResult)super.clone();
        queryPfopResult.resultFileIndex = resultFileIndex;
        queryPfopResult.mediaManager = new MediaManager();
        queryPfopResult.fileMap = new FileMap();
        try {
            queryPfopResult.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryPfopResult;
    }

    public void setBatch(boolean batch) {}

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getProcessName() {
        return this.processName;
    }

    public String getInfo() {
        return "";
    }

    public PfopResult singleWithRetry(Map<String, String> line, int retryCount) throws QiniuException {

        PfopResult pfopResult = null;
        try {
            pfopResult = mediaManager.getPfopResultById(line.get("0"));
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    pfopResult = mediaManager.getPfopResultById(line.get("0"));
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return pfopResult;
    }

    public void processLine(List<Map<String, String>> lineList) throws QiniuException {

        lineList = lineList == null ? null : lineList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (lineList == null || lineList.size() == 0) return;
        List<String> resultList = new ArrayList<>();
        for (Map<String, String> line : lineList) {
            try {
                PfopResult pfopResult = singleWithRetry(line, retryCount);
                if (pfopResult != null)resultList.add(line.toString() + "\t" + JsonConvertUtils.toJson(pfopResult));
                else throw new QiniuException(null, "empty pfop result");
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, processName, getInfo() + "\t" + line.toString());
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
