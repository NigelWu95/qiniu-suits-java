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
    private String urlIndex;
    private Auth auth;
    private long expires;
    private String processName;
    protected String resultPath;
    private int resultIndex;
    private FileMap fileMap;

    public PrivateUrl(Auth auth, String domain, String protocol, String urlIndex, long expires, String resultPath,
                      int resultIndex) throws IOException {
        this.processName = "privateurl";
        this.auth = auth;
        if (urlIndex== null || "".equals(urlIndex)) {
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
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap();
        this.fileMap.initWriter(resultPath, processName, resultIndex);
    }

    public PrivateUrl(Auth auth, String domain, String protocol, String urlIndex, long expires, String resultPath)
            throws IOException {
        this(auth, domain, protocol, urlIndex, expires, resultPath, 0);
    }

    public PrivateUrl clone() throws CloneNotSupportedException {
        PrivateUrl queryAvinfo = (PrivateUrl)super.clone();
        queryAvinfo.fileMap = new FileMap();
        try {
            queryAvinfo.fileMap.initWriter(resultPath, processName, resultIndex++);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryAvinfo;
    }

    public String getProcessName() {
        return this.processName;
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        List<String> resultList = new ArrayList<>();
        String url;
        for (Map<String, String> line : lineList) {
            try {
                url = urlIndex != null ? line.get(urlIndex) : protocol + "://" + domain + "/" + line.get("key");
                String signedUrl = auth.privateDownloadUrl(url, expires);
                if (signedUrl != null) resultList.add(signedUrl);
                else fileMap.writeError( String.valueOf(line) + "\tempty signed url");
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
