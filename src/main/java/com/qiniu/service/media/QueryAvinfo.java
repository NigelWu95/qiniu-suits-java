package com.qiniu.service.media;

import com.qiniu.common.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.model.media.Avinfo;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class QueryAvinfo implements ILineProcess<FileInfo>, Cloneable {

    private String domain;
    private MediaManager mediaManager;
    private String processName;
    private int retryCount = 3;
    protected String resultFileDir;
    private FileMap fileMap;

    private void initBaseParams(String domain) {
        this.processName = "avinfo";
        this.domain = domain;
    }

    public QueryAvinfo(String domain, String resultFileDir) {
        initBaseParams(domain);
        this.resultFileDir = resultFileDir;
        this.mediaManager = new MediaManager();
        this.fileMap = new FileMap();
    }

    public QueryAvinfo(String domain, String resultFileDir, int resultFileIndex) throws IOException {
        this(domain, resultFileDir);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public QueryAvinfo getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        QueryAvinfo queryAvinfo = (QueryAvinfo)super.clone();
        queryAvinfo.mediaManager = new MediaManager();
        queryAvinfo.fileMap = new FileMap();
        try {
            queryAvinfo.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryAvinfo;
    }

    public void setBatch(boolean batch) {}

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getProcessName() {
        return this.processName;
    }

    public String getInfo() {
        return domain;
    }

    public Avinfo singleWithRetry(FileInfo fileInfo, int retryCount) throws QiniuException {

        Avinfo avinfo = null;
        try {
            avinfo = mediaManager.getAvinfo(domain, fileInfo.key);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    avinfo = mediaManager.getAvinfo(domain, fileInfo.key);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return avinfo;
    }

    public void processLine(List<FileInfo> fileInfoList) throws QiniuException {

        fileInfoList = fileInfoList == null ? null : fileInfoList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (fileInfoList == null || fileInfoList.size() == 0) return;
        List<String> resultList = new ArrayList<>();
        for (FileInfo fileInfo : fileInfoList) {
            try {
                Avinfo avinfo = singleWithRetry(fileInfo, retryCount);
                if (avinfo != null) resultList.add(fileInfo.key + "\t" + JsonConvertUtils.toJson(avinfo));
                else throw new QiniuException(null, "empty avinfo");
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, processName, getInfo() + "\t" + fileInfo.key);
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}