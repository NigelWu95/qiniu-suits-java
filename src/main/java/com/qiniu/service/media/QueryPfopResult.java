package com.qiniu.service.media;

import com.qiniu.common.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.model.media.PfopResult;
import com.qiniu.service.interfaces.IQossProcess;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class QueryPfopResult implements IQossProcess, Cloneable {

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

    public PfopResult singleWithRetry(FileInfo fileInfo, int retryCount) throws QiniuException {

        PfopResult pfopResult = null;
        try {
            pfopResult = mediaManager.getPfopResultById(fileInfo.key);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    pfopResult = mediaManager.getPfopResultById(fileInfo.key);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return pfopResult;
    }

    public void processFile(List<FileInfo> fileInfoList, int retryCount) throws QiniuException {

        fileInfoList = fileInfoList == null ? null : fileInfoList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (fileInfoList == null || fileInfoList.size() == 0) return;
        List<String> resultList = new ArrayList<>();
        for (FileInfo fileInfo : fileInfoList) {
            try {
                PfopResult pfopResult = singleWithRetry(fileInfo, retryCount);
                resultList.add(JsonConvertUtils.toJsonWithoutUrlEscape(pfopResult));
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, processName, getInfo() +
                        "\t" + fileInfo.key);
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
