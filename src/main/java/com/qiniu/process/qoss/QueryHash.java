package com.qiniu.process.qoss;

import com.google.gson.JsonParseException;
import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.Map;

public class QueryHash extends Base {

    private String algorithm;
    private String domain;
    private String protocol;
    private String urlIndex;
    private FileChecker fileChecker;

    public QueryHash(Configuration configuration, String algorithm, String protocol, String domain, String urlIndex,
                     String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("qhash", "", "", configuration, null, rmPrefix, savePath, saveIndex);
        set(algorithm, protocol, domain, urlIndex);
        this.fileChecker = new FileChecker(configuration.clone(), algorithm, protocol);
    }

    public void updateQuery(String algorithm, String protocol, String domain, String urlIndex, String rmPrefix)
            throws IOException {
        set(algorithm, protocol, domain, urlIndex);
        this.rmPrefix = rmPrefix;
    }

    private void set(String algorithm, String protocol, String domain, String urlIndex) throws IOException {
        this.algorithm = algorithm;
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = null;
            if (domain == null || "".equals(domain)) {
                throw new IOException("please set one of domain and urlIndex.");
            } else {
                RequestUtils.checkHost(domain);
                this.domain = domain;
                this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            }
        } else {
            this.urlIndex = urlIndex;
        }
    }

    public QueryHash(Configuration configuration, String algorithm, String protocol, String domain, String urlIndex,
                     String rmPrefix, String savePath) throws IOException {
        this(configuration, algorithm, protocol, domain, urlIndex, rmPrefix, savePath, 0);
    }

    public QueryHash clone() throws CloneNotSupportedException {
        QueryHash queryHash = (QueryHash)super.clone();
        queryHash.fileChecker = new FileChecker(configuration.clone(), algorithm, protocol);
        return queryHash;
    }

    @Override
    protected Map<String, String> formatLine(Map<String, String> line) throws IOException {
        if (urlIndex == null) {
            line.put("key", FileNameUtils.rmPrefix(rmPrefix, line.get("key")));
            urlIndex = "url";
            line.put(urlIndex, protocol + "://" + domain + "/" + line.get("key").replaceAll("\\?", "%3F"));
        }
        return line;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get(urlIndex);
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        String qhash = fileChecker.getQHashBody(line.get(urlIndex));
        if (qhash != null && !"".equals(qhash)) {
            // 由于响应的 body 为多行需经过格式化处理为一行字符串
            try {
                return JsonConvertUtils.toJson(qhash);
            } catch (JsonParseException e) {
                throw new QiniuException(e);
            }
        } else {
            throw new QiniuException(null, "0, empty_result");
        }
    }
}
