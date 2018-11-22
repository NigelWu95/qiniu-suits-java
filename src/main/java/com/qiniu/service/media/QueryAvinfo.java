package com.qiniu.service.media;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.model.media.Avinfo;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class QueryAvinfo implements IOssFileProcess {

    private String domain;
    private MediaManager mediaManager;
    protected String processName;
    protected boolean batch = true;
    protected int retryCount = 3;
    protected String resultFileDir;
    protected FileReaderAndWriterMap fileReaderAndWriterMap;

    private void initBaseParams(String domain) {
        this.processName = "avinfo";
        this.domain = domain;
    }

    public QueryAvinfo(String domain, String resultFileDir, int resultFileIndex) throws IOException {
        initBaseParams(domain);
        this.mediaManager = new MediaManager();
        this.mediaManager.setDomain(domain);
        this.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public QueryAvinfo(String domain, String resultFileDir) throws IOException {
        initBaseParams(domain);
        this.mediaManager = new MediaManager();
        this.fileReaderAndWriterMap = new FileReaderAndWriterMap();
    }

    public IOssFileProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        QueryAvinfo queryAvinfo = (QueryAvinfo)super.clone();
        queryAvinfo.mediaManager = new MediaManager();
        try {
            queryAvinfo.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryAvinfo;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getProcessName() {
        return this.processName;
    }

    public String getInfo() {
        return domain;
    }

    public void processFile(List<FileInfo> fileInfoList, int retryCount) throws QiniuException {

        fileInfoList = fileInfoList == null ? null : fileInfoList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (fileInfoList == null || fileInfoList.size() == 0) return;

        List<String> resultList = new ArrayList<>();
        for (FileInfo fileInfo : fileInfoList) {
            try {
                Avinfo avinfo = mediaManager.getAvinfo(fileInfo);
                String result = JsonConvertUtils.toJsonWithoutUrlEscape(avinfo);
                if (!StringUtils.isNullOrEmpty(result)) resultList.add(result);
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileReaderAndWriterMap, processName, getInfo() +
                        "\t" + fileInfo.key);
            }
        }
        if (resultList.size() > 0) fileReaderAndWriterMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {

    }
}