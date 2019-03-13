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
    final private String accessKey;
    final private String secretKey;
    final private String algorithm;
    private FileChecker fileChecker;
    final private String processName;
    private int retryCount;
    final private String savePath;
    private String saveTag;
    private int saveIndex;
    private FileMap fileMap;

    public QueryHash(String domain, String algorithm, String protocol, String urlIndex, String accessKey, String secretKey,
                     String savePath, int saveIndex) throws IOException {
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
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.fileChecker = new FileChecker(algorithm, protocol, accessKey == null ? null :
                Auth.create(accessKey, secretKey));
        this.savePath = savePath;
        this.saveTag = "";
        this.saveIndex = saveIndex;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public QueryHash(String domain, String algorithm, String protocol, String urlIndex, String accessKey, String secretKey,
                     String savePath) throws IOException {
        this(domain, algorithm, protocol, urlIndex, accessKey, secretKey, savePath, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount < 1 ? 1 : retryCount;
    }

    public void setSaveTag(String saveTag) {
        this.saveTag = saveTag == null ? "" : saveTag;
    }

    public QueryHash clone() throws CloneNotSupportedException {
        QueryHash queryHash = (QueryHash)super.clone();
        queryHash.fileChecker = new FileChecker(algorithm, protocol, accessKey == null ? null :
                Auth.create(accessKey, secretKey));
        queryHash.fileMap = new FileMap(savePath, processName, saveTag + String.valueOf(++saveIndex));
        try {
            queryHash.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryHash;
    }

    public void processLine(List<Map<String, String>> lineList, int retryCount) throws IOException {
        String url;
        String key;
        String qhash = null;
        JsonParser jsonParser = new JsonParser();
        int retry;
        for (Map<String, String> line : lineList) {
            if (urlIndex != null) {
                url = line.get(urlIndex);
                key = url.split("(https?://[^\\s/]+\\.[^\\s/.]{1,3}/)|(\\?.+)")[1];
            } else  {
                url = protocol + "://" + domain + "/" + line.get("key");
                key = line.get("key");
            }
            retry = retryCount;
            while (retry > 0) {
                try {
                    qhash = fileChecker.getQHashBody(url);
                    retry = 0;
                } catch (QiniuException e) {
                    retry--;
                    String finalKey = key + "\t" + url;
                    HttpResponseUtils.processException(e, retry, fileMap, new ArrayList<String>(){{ add(finalKey); }});
                }
            }
            if (qhash != null && !"".equals(qhash)) {
                fileMap.writeSuccess(key + "\t" + url + "\t" + jsonParser.parse(qhash).toString(), false);
            } else {
                fileMap.writeError( key + "\t" + url + "\tempty qhash", false);
            }
        }
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        processLine(lineList, retryCount);
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
