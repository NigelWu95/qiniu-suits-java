package com.qiniu.service.media;

import com.google.gson.JsonParser;
import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.*;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryAvinfo implements ILineProcess<Map<String, String>>, Cloneable {

    final private String processName;
    private String domain;
    private String protocol;
    final private String urlIndex;
    final private String accessKey;
    final private String secretKey;
    private MediaManager mediaManager;
    private int retryCount;
    final private String savePath;
    private String saveTag;
    private int saveIndex;
    private FileMap fileMap;

    public QueryAvinfo(String domain, String protocol, String urlIndex, String accessKey, String secretKey,
                       String savePath, int saveIndex) throws IOException {
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
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.mediaManager = new MediaManager(protocol, accessKey == null ? null :
                Auth.create(accessKey, secretKey));
        this.savePath = savePath;
        this.saveTag = "";
        this.saveIndex = saveIndex;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public QueryAvinfo(String domain, String protocol, String urlIndex, String accessKey, String secretKey,
                       String savePath) throws IOException {
        this(domain, protocol, urlIndex, accessKey, secretKey, savePath, 0);
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

    public QueryAvinfo clone() throws CloneNotSupportedException {
        QueryAvinfo queryAvinfo = (QueryAvinfo)super.clone();
        queryAvinfo.mediaManager = new MediaManager(protocol, accessKey == null ? null :
                Auth.create(accessKey, secretKey));
        queryAvinfo.fileMap = new FileMap(savePath, processName, saveTag + String.valueOf(++saveIndex));
        try {
            queryAvinfo.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryAvinfo;
    }

    public void processLine(List<Map<String, String>> lineList, int retryCount) throws IOException {
        String url;
        String key;
        String avinfo;
        JsonParser jsonParser = new JsonParser();
        int retry;
        for (Map<String, String> line : lineList) {
            if (urlIndex != null) {
                url = line.get(urlIndex);
                try {
                    key = URLUtils.getKey(url);
                } catch (IOException e) {
                    fileMap.writeError(String.valueOf(line) + "\t" + e.getMessage(), false);
                    continue;
                }
            } else {
                key = line.get("key").replaceAll("\\?", "%3F");
                url = protocol + "://" + domain + "/" + key;
            }
            String finalInfo = key + "\t" + url;
            retry = retryCount;
            while (retry > 0) {
                try {
                    avinfo = mediaManager.getAvinfoBody(url);
                    if (avinfo != null && !"".equals(avinfo))
                        // 由于响应的 body 经过格式化通过 JsonParser 处理为一行字符串
                        fileMap.writeSuccess(finalInfo + "\t" + jsonParser.parse(avinfo).toString(), false);
                    else
                        // 因为需要经过 JsonParser 处理，进行下控制判断，避免抛出异常
                        fileMap.writeKeyFile("empty_result", finalInfo, false);
                    retry = 0;
                } catch (QiniuException e) {
                    retry--;
                    retry = HttpResponseUtils.processException(e, retry, fileMap, new ArrayList<String>(){{
                        add(finalInfo);
                    }});
                }
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
