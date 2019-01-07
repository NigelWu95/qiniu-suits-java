package com.qiniu.service.media;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryAvinfo implements ILineProcess<Map<String, String>>, Cloneable {

    private String domain;
    private String protocol;
    private String urlIndex;
    private Auth auth;
    private MediaManager mediaManager;
    private String processName;
    private int retryCount;
    protected String resultPath;
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
        this.retryCount = retryCount;
    }

    public void setResultTag(String resultTag) {
        this.resultTag = resultTag == null ? "" : resultTag;
    }

    public QueryAvinfo clone() throws CloneNotSupportedException {
        QueryAvinfo queryAvinfo = (QueryAvinfo)super.clone();
        queryAvinfo.mediaManager = new MediaManager(protocol, auth);
        queryAvinfo.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(resultIndex++));
        try {
            queryAvinfo.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryAvinfo;
    }

    public String singleWithRetry(String url, int retryCount) throws QiniuException {

        String avinfo = null;
        try {
            avinfo = mediaManager.getAvinfoBody(url);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    avinfo = mediaManager.getAvinfoBody(url);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }
        return avinfo;
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        List<String> resultList = new ArrayList<>();
        String url;
        JsonParser jsonParser = new JsonParser();
        for (Map<String, String> line : lineList) {
            try {
                url = urlIndex != null ? line.get(urlIndex) : protocol + "://" + domain + "/" + line.get("key");
                String avinfo = singleWithRetry(url, retryCount);
                if (avinfo != null) resultList.add(line.get("key") + "\t" + jsonParser.parse(avinfo).toString());
                else fileMap.writeError( String.valueOf(line) + "\tpfop avinfo");
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, new ArrayList<String>(){{add(String.valueOf(line));}});
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
