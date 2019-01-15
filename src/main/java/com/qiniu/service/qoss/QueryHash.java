package com.qiniu.service.qoss;

import com.google.gson.JsonParser;
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
    final private String urlIndex;
    final private Auth auth;
    final private String algorithm;
    private FileChecker fileChecker;
    final private String processName;
    private int retryCount;
    final private String resultPath;
    private String resultTag;
    private int resultIndex;
    private FileMap fileMap;

    public QueryHash(String domain, String algorithm, String protocol, String urlIndex, Auth auth, String resultPath,
                     int resultIndex)
            throws IOException {
        this.processName = "qhash";
        if (urlIndex == null || "".equals(urlIndex)) {
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
        this.resultTag = "";
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap(resultPath, processName, String.valueOf(resultIndex));
        this.fileMap.initDefaultWriters();
    }

    public QueryHash(String domain, String algorithm, String protocol, String urlIndex, Auth auth, String resultPath)
            throws IOException {
        this(domain, algorithm, protocol, urlIndex, auth, resultPath, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount < 1 ? 1 : retryCount;
    }

    public void setResultTag(String resultTag) {
        this.resultTag = resultTag == null ? "" : resultTag;
    }

    public QueryHash clone() throws CloneNotSupportedException {
        QueryHash queryHash = (QueryHash)super.clone();
        queryHash.fileChecker = new FileChecker(algorithm, protocol, auth);
        queryHash.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(++resultIndex));
        try {
            queryHash.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryHash;
    }

    public String singleWithRetry(String url, int retryCount) throws QiniuException {
        String qhash = null;
        while (retryCount > 0) {
            try {
                qhash = fileChecker.getQHashBody(url);
                retryCount = 0;
            } catch (QiniuException e2) {
                retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
            }
        }
        return qhash;
    }

    public void processLine(List<Map<String, String>> lineList, int retryCount) throws QiniuException {
        String url;
        String key;
        String qhash;
        JsonParser jsonParser = new JsonParser();
        for (Map<String, String> line : lineList) {
            if (urlIndex != null) {
                url = line.get(urlIndex);
                key = url.split("(https?://[^\\s/]+\\.[^\\s/.]{1,3}/)|(\\?.+)")[1];
            } else  {
                url = protocol + "://" + domain + "/" + line.get("key");
                key = line.get("key");
            }
            try {
                qhash = singleWithRetry(url, retryCount);
                if (qhash != null && !"".equals(qhash))
                    fileMap.writeSuccess(key + "\t" + url + "\t" + jsonParser.parse(qhash).toString());
                else
                    fileMap.writeError( key + "\t" + url + "\tempty qhash");
            } catch (QiniuException e) {
                String finalKey = key + "\t" + url;
                HttpResponseUtils.processException(e, fileMap, new ArrayList<String>(){{add(finalKey);}});
            }
        }
    }

    public void processLine(List<Map<String, String>> lineList) throws QiniuException {
        processLine(lineList, retryCount);
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
