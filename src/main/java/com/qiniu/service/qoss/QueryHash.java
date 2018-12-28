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
import java.util.stream.Collectors;

public class QueryHash implements ILineProcess<Map<String, String>>, Cloneable {

    private String domain;
    private String protocol;
    private String urlIndex;
    private Auth auth;
    private String algorithm;
    private FileChecker fileChecker;
    private String processName;
    private int retryCount;
    protected String resultPath;
    private int resultIndex;
    private FileMap fileMap;

    public QueryHash(String domain, String algorithm, String protocol, String urlIndex, Auth auth, String resultPath,
                     int resultIndex)
            throws IOException {
        this.processName = "qhash";
        if (urlIndex== null || "".equals(urlIndex)) {
            this.urlIndex = null;
            if (domain == null || "".equals(domain)) throw new IOException("please set one of domain and urlIndex.");
            else {
                RequestUtils.checkHost(domain);
                this.domain = domain;
                this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            }
        } else this.urlIndex = urlIndex;
        this.algorithm = algorithm;
        this.auth = auth;
        this.fileChecker = new FileChecker(algorithm, protocol, auth);
        this.resultPath = resultPath;
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap(resultPath, processName, String.valueOf(resultIndex));
        this.fileMap.initDefaultWriters();
    }

    public QueryHash(String domain, String algorithm, String protocol, String urlIndex, Auth auth, String resultPath)
            throws IOException {
        this(domain, algorithm, protocol, urlIndex, auth, resultPath, 0);
    }

    public QueryHash clone() throws CloneNotSupportedException {
        QueryHash queryHash = (QueryHash)super.clone();
        queryHash.fileChecker = new FileChecker(algorithm, protocol, auth);
        queryHash.fileMap = new FileMap(resultPath, processName, String.valueOf(resultIndex++));
        try {
            queryHash.fileMap.initDefaultWriters();
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

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        List<String> resultList = new ArrayList<>();
        String url;
        for (Map<String, String> line : lineList) {
            try {
                url = urlIndex != null ? line.get(urlIndex) : protocol + "://" + domain + "/" + line.get("key");
                String qhash = singleWithRetry(url, retryCount);
                if (qhash != null) resultList.add(url + "\t" + qhash);
                else fileMap.writeError( String.valueOf(line) + "\tempty qhash");
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, String.valueOf(line));
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
