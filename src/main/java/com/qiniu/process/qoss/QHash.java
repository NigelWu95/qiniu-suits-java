package com.qiniu.process.qoss;

import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.process.Base;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class QHash extends Base {

    private String domain;
    private String protocol;
    final private String urlIndex;
    final private String accessKey;
    final private String secretKey;
    final private String algorithm;
    private FileChecker fileChecker;
    private JsonParser jsonParser;

    public QHash(String domain, String algorithm, String protocol, String urlIndex, String accessKey, String secretKey,
                 String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("qhash", accessKey, secretKey, null, null, rmPrefix, savePath, saveIndex);
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
        this.jsonParser = new JsonParser();
    }

    public QHash(String domain, String algorithm, String protocol, String urlIndex, String accessKey, String secretKey,
                 String rmPrefix, String savePath) throws IOException {
        this(domain, algorithm, protocol, urlIndex, accessKey, secretKey, rmPrefix, savePath, 0);
    }

    public QHash clone() throws CloneNotSupportedException {
        QHash queryHash = (QHash)super.clone();
        queryHash.fileChecker = new FileChecker(algorithm, protocol, accessKey == null ? null :
                Auth.create(accessKey, secretKey));
        queryHash.jsonParser = new JsonParser();
        return queryHash;
    }

    protected Map<String, String> formatLine(Map<String, String> line) throws IOException {
        if (urlIndex != null) {
            line.put("key", URLUtils.getKey(line.get(urlIndex)));
        } else  {
            line.put("key", FileNameUtils.rmPrefix(rmPrefix, line.get("key"))
                    .replaceAll("\\?", "%3F"));
            line.put("url", protocol + "://" + domain + "/" + line.get("key"));
        }
        return line;
    }

    protected String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get("url");
    }

    protected Response batchResult(List<Map<String, String>> lineList) {
        return null;
    }

    protected String singleResult(Map<String, String> line) throws QiniuException {
        String qhash = fileChecker.getQHashBody(line.get("url"));
        if (qhash != null && !"".equals(qhash)) {
            // 由于响应的 body 经过格式化通过 JsonParser 处理为一行字符串
            try {
                return jsonParser.parse(qhash).toString();
            } catch (JsonParseException e) {
                throw new QiniuException(e);
            }
        } else {
            throw new QiniuException(null, "empty_result");
        }
    }
}
