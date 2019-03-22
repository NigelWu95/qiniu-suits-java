package com.qiniu.process.qdora;

import com.google.gson.JsonParser;
import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryAvinfo implements ILineProcess<Map<String, String>>, Cloneable {

    final private String processName;
    private String domain;
    private String protocol;
    final private String urlIndex;
    final private String accessKey;
    final private String secretKey;
    private MediaManager mediaManager;
    final private String rmPrefix;
    private int retryTimes = 3;
    final private String savePath;
    private String saveTag;
    private int saveIndex;
    private FileMap fileMap;

    public QueryAvinfo(String domain, String protocol, String urlIndex, String accessKey, String secretKey,
                       String rmPrefix, String savePath, int saveIndex) throws IOException {
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
        this.rmPrefix = rmPrefix;
        this.savePath = savePath;
        this.saveTag = "";
        this.saveIndex = saveIndex;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public QueryAvinfo(String domain, String protocol, String urlIndex, String accessKey, String secretKey,
                       String rmPrefix, String savePath) throws IOException {
        this(domain, protocol, urlIndex, accessKey, secretKey, rmPrefix, savePath, 0);
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

    public void processLine(List<Map<String, String>> lineList, int retryTimes) throws IOException {
        String url;
        String key;
        String avinfo;
        JsonParser jsonParser = new JsonParser();
        int retry;
        Map<String, String> line;
        for (int i = 0; i < lineList.size(); i++) {
            line = lineList.get(i);
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
                    avinfo = mediaManager.getAvinfoBody(url);
                    if (avinfo != null && !"".equals(avinfo))
                        // 由于响应的 body 经过格式化通过 JsonParser 处理为一行字符串
                        fileMap.writeSuccess(finalInfo + "\t" + jsonParser.parse(avinfo).toString(), false);
                    else
                        // 因为需要经过 JsonParser 处理，进行下控制判断，避免抛出异常
                        fileMap.writeKeyFile("empty_result", finalInfo, false);
                    retry = 0;
                } catch (QiniuException e) {
                    retry = HttpResponseUtils.checkException(e, retry);
                    if (retry == 0) LogUtils.writeLog(e, fileMap, line.get("key"));
                    else if (retry == -1) {
                        LogUtils.writeLog(e, fileMap, lineList.subList(i, lineList.size() - 1).parallelStream()
                                .map(String::valueOf).collect(Collectors.toList()));
                        throw e;
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
