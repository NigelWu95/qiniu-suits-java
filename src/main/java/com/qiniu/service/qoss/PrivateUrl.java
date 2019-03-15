package com.qiniu.service.qoss;

import com.qiniu.persistence.FileMap;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.Auth;
import com.qiniu.util.RequestUtils;
import com.qiniu.util.URLUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PrivateUrl implements ILineProcess<Map<String, String>>, Cloneable {

    private String domain;
    private String protocol;
    final private String urlIndex;
    final private String accessKey;
    final private String secretKey;
    private Auth auth;
    final private long expires;
    final private String processName;
    final private String savePath;
    private String saveTag;
    private int saveIndex;
    private FileMap fileMap;

    public PrivateUrl(String accessKey, String secretKey, String domain, String protocol, String urlIndex, long expires,
                      String savePath, int saveIndex) throws IOException {
        this.processName = "privateurl";
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.auth = Auth.create(accessKey, secretKey);
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = null;
            if (domain == null || "".equals(domain)) throw new IOException("please set one of domain and urlIndex.");
            else {
                RequestUtils.checkHost(domain);
                this.domain = domain;
                this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            }
        } else this.urlIndex = urlIndex;
        this.expires = expires == 0L ? 3600 : expires;
        this.savePath = savePath;
        this.saveTag = "";
        this.saveIndex = saveIndex;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public PrivateUrl(String accessKey, String secretKey, String domain, String protocol, String urlIndex, long expires,
                      String savePath) throws IOException {
        this(accessKey, secretKey, domain, protocol, urlIndex, expires, savePath, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setSaveTag(String saveTag) {
        this.saveTag = saveTag == null ? "" : saveTag;
    }

    public PrivateUrl clone() throws CloneNotSupportedException {
        PrivateUrl privateUrl = (PrivateUrl)super.clone();
        privateUrl.auth = Auth.create(accessKey, secretKey);
        privateUrl.fileMap = new FileMap(savePath, processName, saveTag + String.valueOf(++saveIndex));
        try {
            privateUrl.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return privateUrl;
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        String url;
        String key;
        String signedUrl;
        for (Map<String, String> line : lineList) {
            if (urlIndex != null) {
                url = line.get(urlIndex);
                try {
                    key = URLUtils.getKey(url);
                } catch (IOException e) {
                    fileMap.writeError(String.valueOf(line) + "\t" + e.getMessage(), false);
                    continue;
                }
            } else  {
                key = line.get("key").replaceAll("\\?", "%3F");
                url = protocol + "://" + domain + "/" + key;
            }
            String finalInfo = key + "\t" + url;
            signedUrl = auth.privateDownloadUrl(url, expires);
            fileMap.writeSuccess(finalInfo + "\t" + signedUrl, false);
        }
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
