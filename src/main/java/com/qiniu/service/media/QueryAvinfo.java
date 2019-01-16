package com.qiniu.service.media;

import com.google.gson.JsonParser;
import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryAvinfo implements ILineProcess<Map<String, String>>, Cloneable {

    final private String processName;
    private String domain;
    private String protocol;
    final private String urlIndex;
    final private Auth auth;
    private MediaManager mediaManager;
    private int retryCount;
    final private String resultPath;
    private String resultTag;
    private int resultIndex;
    private FileMap fileMap;

    public QueryAvinfo(String domain, String protocol, String urlIndex, Auth auth, String resultPath, int resultIndex)
            throws IOException {
        this.processName = "avinfo";
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = null;
            if (domain == null || "".equals(domain)) throw new IOException("please set one of domain and urlIndex.");
            else {
                RequestUtils.checkHost(domain);
                this.domain = domain;
                this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            }
        } else this.urlIndex = urlIndex;
        this.auth = auth;
        this.mediaManager = new MediaManager(protocol, auth);
        this.resultPath = resultPath;
        this.resultTag = "";
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap(resultPath, processName, String.valueOf(resultIndex));
        this.fileMap.initDefaultWriters();
    }

    public QueryAvinfo(String domain, String protocol, String urlIndex, Auth auth, String resultPath) throws IOException {
        this(domain, protocol, urlIndex, auth, resultPath, 0);
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

    public QueryAvinfo clone() throws CloneNotSupportedException {
        QueryAvinfo queryAvinfo = (QueryAvinfo)super.clone();
        queryAvinfo.mediaManager = new MediaManager(protocol, auth);
        queryAvinfo.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(++resultIndex));
        try {
            queryAvinfo.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryAvinfo;
    }

    public void processLine(List<Map<String, String>> lineList, int retryCount) throws QiniuException {
        String url;
        String key;
        String avinfo = null;
        JsonParser jsonParser = new JsonParser();
        for (Map<String, String> line : lineList) {
            if (urlIndex != null) {
                url = line.get(urlIndex);
                key = url.split("(https?://[^\\s/]+/)|(\\?)")[1];
            } else {
                url = protocol + "://" + domain + "/" + line.get("key");
                key = line.get("key");
            }
            int retry = retryCount;
            while (retry > 0) {
                try {
                    avinfo = mediaManager.getAvinfoBody(url);
                    retry = 0;
                } catch (QiniuException e) {
                    retry--;
                    String finalKey = key + "\t" + url;
                    HttpResponseUtils.processException(e, retry, fileMap, new ArrayList<String>(){{ add(finalKey); }});
                }
            }
            if (avinfo != null && !"".equals(avinfo))
                fileMap.writeSuccess(key + "\t" + url + "\t" + jsonParser.parse(avinfo).toString());
            else
                fileMap.writeError( key + "\t" + url + "\tempty avinfo");
        }
    }

    public void processLine(List<Map<String, String>> lineList) throws QiniuException {
        processLine(lineList, retryCount);
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
