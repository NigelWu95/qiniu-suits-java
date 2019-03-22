package com.qiniu.service.qoss;

import com.google.gson.JsonParser;
import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.*;

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
    final private String rmPrefix;
    final private String processName;
    private int retryTimes = 3;
    final private String savePath;
    private String saveTag;
    private int saveIndex;
    private FileMap fileMap;

    public QueryHash(String domain, String algorithm, String protocol, String urlIndex, String accessKey, String secretKey,
                     String rmPrefix, String savePath, int saveIndex) throws IOException {
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
        this.rmPrefix = rmPrefix;
        this.savePath = savePath;
        this.saveTag = "";
        this.saveIndex = saveIndex;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public QueryHash(String domain, String algorithm, String protocol, String urlIndex, String accessKey, String secretKey,
                     String rmPrefix, String savePath) throws IOException {
        this(domain, algorithm, protocol, urlIndex, accessKey, secretKey, rmPrefix, savePath, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes < 1 ? 3 : retryTimes;
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

    public void processLine(List<Map<String, String>> lineList, int retryTimes) throws IOException {
        String url;
        String key;
        String qhash;
        JsonParser jsonParser = new JsonParser();
        int retry;
        for (Map<String, String> line : lineList) {
            try {
                if (urlIndex != null) {
                    url = line.get(urlIndex);
                    key = URLUtils.getKey(url);
                } else  {
                    key = FileNameUtils.rmPrefix(rmPrefix, line.get("key")).replaceAll("\\?", "%3F");
                    url = protocol + "://" + domain + "/" + key;
                }
            } catch (IOException e) {
                fileMap.writeError(String.valueOf(line) + "\t" + e.getMessage(), false);
                continue;
            }
            String finalInfo = key + "\t" + url;
            retry = retryTimes;
            while (retry > 0) {
                try {
                    qhash = fileChecker.getQHashBody(url);
                    if (qhash != null && !"".equals(qhash)) {
                        // 由于响应的 body 经过格式化通过 JsonParser 处理为一行字符串
                        fileMap.writeSuccess(finalInfo + "\t" + jsonParser.parse(qhash).toString(), false);
                    } else {
                        // 因为需要经过 JsonParser 处理，进行下控制判断，避免抛出异常
                        fileMap.writeKeyFile("empty_result", finalInfo, false);
                    }
                    retry = 0;
                } catch (QiniuException e) {
                    retry = HttpResponseUtils.checkException(e, retry);
                    if (retry < 1) {
                        HttpResponseUtils.writeLog(e, fileMap, finalInfo);
                        if (retry == -1) throw e;
                    }
                }
            }
        }
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        processLine(lineList, retryTimes);
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
