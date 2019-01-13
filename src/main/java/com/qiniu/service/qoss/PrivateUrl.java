package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.persistence.FileMap;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.media.MediaManager;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.RequestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PrivateUrl implements ILineProcess<Map<String, String>>, Cloneable {

    private String domain;
    private String protocol;
    final private String urlIndex;
    final private Auth auth;
    final private long expires;
    final private String processName;
    final private String resultPath;
    private String resultTag;
    private int resultIndex;
    private FileMap fileMap;

    public PrivateUrl(Auth auth, String domain, String protocol, String urlIndex, long expires, String resultPath,
                      int resultIndex) throws IOException {
        this.processName = "privateurl";
        this.auth = auth;
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
        this.resultPath = resultPath;
        this.resultTag = "";
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap(resultPath, processName, String.valueOf(resultIndex));
        this.fileMap.initDefaultWriters();
    }

    public PrivateUrl(Auth auth, String domain, String protocol, String urlIndex, long expires, String resultPath)
            throws IOException {
        this(auth, domain, protocol, urlIndex, expires, resultPath, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setResultTag(String resultTag) {
        this.resultTag = resultTag == null ? "" : resultTag;
    }

    public PrivateUrl clone() throws CloneNotSupportedException {
        PrivateUrl queryAvinfo = (PrivateUrl)super.clone();
        queryAvinfo.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(++resultIndex));
        try {
            queryAvinfo.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryAvinfo;
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        String url;
        String key;
        String signedUrl;
        for (Map<String, String> line : lineList) {
            if (urlIndex != null) {
                url = line.get(urlIndex);
                key = url.split("(https?://[^\\s/]+\\.[^\\s/.]{1,3}/)|(\\?.+)")[1];
            } else  {
                url = protocol + "://" + domain + "/" + line.get("key");
                key = line.get("key");
            }
            try {
                signedUrl = auth.privateDownloadUrl(url, expires);
                if (signedUrl != null && !"".equals(signedUrl))
                    fileMap.writeSuccess(key + "\t" + url + "\t" + signedUrl);
                else
                    fileMap.writeError( key + "\t" + url + "\tempty signed url");
            } catch (QiniuException e) {
                String finalKey = key + "\t" + url;
                HttpResponseUtils.processException(e, fileMap, new ArrayList<String>(){{add(finalKey);}});
            }
        }
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
