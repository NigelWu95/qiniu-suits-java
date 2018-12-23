package com.qiniu.service.qoss;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.RequestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryHash implements ILineProcess<Map<String, String>>, Cloneable {

    private String domain;
    private String protocol;
    private Auth srcAuth;
    private String algorithm;
    private FileChecker fileChecker;
    private String processName;
    private int retryCount;
    protected String resultPath;
    private int resultIndex;
    private FileMap fileMap;

    public QueryHash(String domain, String algorithm, String protocol, Auth srcAuth, String resultPath, int resultIndex)
            throws IOException {
        this.processName = "hash";
        if (domain == null || "".equals(domain)) this.domain = null;
        else {
            RequestUtils.checkHost(domain);
            this.domain = domain;
        }
        this.algorithm = algorithm;
        this.protocol = protocol;
        this.srcAuth = srcAuth;
        this.fileChecker = new FileChecker(algorithm, protocol, srcAuth);
        this.resultPath = resultPath;
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap();
        this.fileMap.initWriter(resultPath, processName, resultIndex);
    }

    public QueryHash(String domain, String algorithm, String protocol, Auth srcAuth, String resultPath)
            throws IOException {
        this(domain, algorithm, protocol, srcAuth, resultPath, 0);
    }

    public QueryHash clone() throws CloneNotSupportedException {
        QueryHash queryHash = (QueryHash)super.clone();
        queryHash.fileChecker = new FileChecker(algorithm, protocol, srcAuth);
        queryHash.fileMap = new FileMap();
        try {
            queryHash.fileMap.initWriter(resultPath, processName, resultIndex++);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryHash;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getProcessName() {
        return this.processName;
    }

    public String singleWithRetry(String url, int retryCount) throws QiniuException {

        String qhash = null;
        try {
            qhash = fileChecker.getQHashBody(url);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    qhash = fileChecker.getQHashBody(url);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return qhash;
    }

    public void processLine(List<Map<String, String>> lineList) throws QiniuException {

        List<String> resultList = new ArrayList<>();
        for (Map<String, String> line : lineList) {
            String url = domain == null ? line.get("url") : protocol + "://" + domain + "/" + line.get("key");
            try {
                String qhash = singleWithRetry(url, retryCount);
                if (qhash != null) resultList.add(url + "\t" + qhash);
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, url);
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
