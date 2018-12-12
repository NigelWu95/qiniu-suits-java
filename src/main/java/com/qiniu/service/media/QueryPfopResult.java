package com.qiniu.service.media;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.model.media.PfopResult;
import com.qiniu.service.convert.PfopResultToString;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryPfopResult implements ILineProcess<Map<String, String>>, Cloneable {

    private MediaManager mediaManager;
    private String processName;
    private int retryCount = 3;
    protected String resultFileDir;
    private FileMap fileMap;
    private ITypeConvert<PfopResult, String> typeConverter;

    private void initBaseParams() {
        this.processName = "pfopresult";
    }

    public QueryPfopResult(String resultFileDir) {
        initBaseParams();
        this.resultFileDir = resultFileDir;
        this.mediaManager = new MediaManager();
        this.fileMap = new FileMap();
    }

    public QueryPfopResult(String resultFileDir, int resultFileIndex) throws IOException {
        this(resultFileDir);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public void setTypeConverter() {
        this.typeConverter = new PfopResultToString();
    }

    public QueryPfopResult getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        QueryPfopResult queryPfopResult = (QueryPfopResult)super.clone();
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
                String pfopResult = singleWithRetry(line.get("persistentId"), retryCount);
                if (pfopResult != null)resultList.add(pfopResult);
                else throw new QiniuException(null, "empty pfop result");
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, processName, getInfo() + "\t" + line.get("persistentId"));
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
