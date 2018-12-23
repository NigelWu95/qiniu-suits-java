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
    private Auth auth;
    private long expires;
    private String processName;
    protected String resultPath;
    private int resultIndex;
    private FileMap fileMap;

    public PrivateUrl(Auth auth, String domain, String protocol, long expires, String resultPath, int resultIndex)
            throws IOException {
        this.processName = "privateurl";
        this.auth = auth;
        if (domain == null || "".equals(domain)) this.domain = null;
        else {
            RequestUtils.checkHost(domain);
            this.domain = domain;
        }
        this.protocol = protocol == null || "".equals(protocol) || !protocol.matches("(http|https)") ? "http" : protocol;
        this.expires = expires == 0L ? 3600 : expires;
        this.resultPath = resultPath;
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap();
        this.fileMap.initWriter(resultPath, processName, resultIndex);
    }

    public PrivateUrl(Auth auth, String domain, String protocol, long expires, String resultPath) throws IOException {
        this(auth, domain, protocol, expires, resultPath, 0);
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

    public void processLine(List<Map<String, String>> lineList) throws QiniuException {

        List<String> resultList = new ArrayList<>();
        for (Map<String, String> line : lineList) {
            String url = domain == null ? line.get("url") : protocol + "://" + domain + "/" + line.get("key");
            try {
                String signedUrl = auth.privateDownloadUrl(url, expires);
                if (signedUrl != null) resultList.add(signedUrl);
                else throw new QiniuException(null, "empty url");
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
