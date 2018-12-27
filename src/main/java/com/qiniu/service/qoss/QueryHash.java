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
        if (domain == null || "".equals(domain)) {
            this.domain = null;
            if (urlIndex== null || "".equals(urlIndex)) throw new IOException("please set one of domain and urlIndex.");
            else this.urlIndex = urlIndex;
        } else {
            RequestUtils.checkHost(domain);
            this.domain = domain;
            this.protocol = protocol == null || "".equals(protocol) || !protocol.matches("(http|https)") ? "http" : protocol;
        }
        this.algorithm = algorithm;
        this.auth = auth;
        this.fileChecker = new FileChecker(algorithm, protocol, auth);
        this.resultPath = resultPath;
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap();
        this.fileMap.initWriter(resultPath, processName, resultIndex);
    }

    public QueryHash(String domain, String algorithm, String protocol, String urlIndex, Auth auth, String resultPath)
            throws IOException {
        this(domain, algorithm, protocol, urlIndex, auth, resultPath, 0);
    }

    public QueryHash clone() throws CloneNotSupportedException {
        QueryHash queryHash = (QueryHash)super.clone();
        queryHash.fileChecker = new FileChecker(algorithm, protocol, auth);
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
        List<String> urlList;
        if (domain != null) {
            List<String> keyList = lineList.stream().map(line -> line.get("key"))
                    .filter(pid -> pid != null && !"".equals(pid)).collect(Collectors.toList());
            if (keyList.size() == 0) throw new QiniuException(null, "there is no key in line.");
            urlList = keyList.stream().map(key -> protocol + "://" + domain + "/" + key).collect(Collectors.toList());
        } else {
            urlList = lineList.stream().map(line -> line.get(urlIndex)).collect(Collectors.toList());
        }
        List<String> resultList = new ArrayList<>();
        for (String url : urlList) {
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
