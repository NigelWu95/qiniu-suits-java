package com.qiniu.service.media;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.model.media.Avinfo;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class QueryAvinfo implements IOssFileProcess, Cloneable {

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
        this.resultFileDir = resultFileDir;
        this.mediaManager = new MediaManager();
        this.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public QueryAvinfo(String domain, String resultFileDir) {
        initBaseParams(domain);
        this.resultFileDir = resultFileDir;
        this.mediaManager = new MediaManager();
        this.fileReaderAndWriterMap = new FileReaderAndWriterMap();
    }

    public IOssFileProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        QueryAvinfo queryAvinfo = (QueryAvinfo)super.clone();
        queryAvinfo.mediaManager = new MediaManager();
        this.fileReaderAndWriterMap = new FileReaderAndWriterMap();
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
        List<String> avinfoList = new ArrayList<>();
        for (FileInfo fileInfo : fileInfoList) {
            try {
                Avinfo avinfo = mediaManager.getAvinfo(domain, fileInfo.key);
                avinfoList.add(mediaManager.getCurrentAvinfoJson());
                int width = avinfo.getVideoStream().width;
                if (width > 1280) {
                    resultList.add(fileInfo.key + "\t" + "avthumb/mp4/s/1920x1080|saveas/" + UrlSafeBase64.encodeToString(
                            ObjectUtils.addPrefixAndSuffixKeepExt("fantasy-tv-avthumb:", fileInfo.key, "F1080")));
                    resultList.add(fileInfo.key + "\t" + "avthumb/mp4/s/1280x720|saveas/" + UrlSafeBase64.encodeToString(
                            ObjectUtils.addPrefixAndSuffixKeepExt("fantasy-tv-avthumb:", fileInfo.key, "F720")));
                    resultList.add(fileInfo.key + "\t" + "avthumb/mp4/s/640x480|saveas/" + UrlSafeBase64.encodeToString(
                            ObjectUtils.addPrefixAndSuffixKeepExt("fantasy-tv-avthumb:", fileInfo.key, "F480")));
                } else if (width > 1000) {
                    resultList.add(fileInfo.key + "\t" + "avthumb/mp4/s/1280x720|saveas/" + UrlSafeBase64.encodeToString(
                            ObjectUtils.addPrefixAndSuffixKeepExt("fantasy-tv-avthumb:", fileInfo.key, "F720")));
                    resultList.add(fileInfo.key + "\t" + "avthumb/mp4/s/640x480|saveas/" + UrlSafeBase64.encodeToString(
                            ObjectUtils.addPrefixAndSuffixKeepExt("fantasy-tv-avthumb:", fileInfo.key, "F480")));
                } else if (width > 640) {
                    resultList.add(fileInfo.key + "\t" + "avthumb/mp4/s/640x480|saveas/" + UrlSafeBase64.encodeToString(
                            ObjectUtils.addPrefixAndSuffixKeepExt("fantasy-tv-avthumb:", fileInfo.key, "F480")));
                } else {
                    int height = avinfo.getVideoStream().height;
                    resultList.add(fileInfo.key + "\t" + "avthumb/mp4/s/" + width + "x" + height + "|saveas/" + UrlSafeBase64.encodeToString(
                            ObjectUtils.addPrefixAndSuffixKeepExt("fantasy-tv-avthumb:", fileInfo.key, "F1080")));
                }
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileReaderAndWriterMap, processName, getInfo() +
                        "\t" + fileInfo.key);
            }
        }
        if (resultList.size() > 0) fileReaderAndWriterMap.writeSuccess(String.join("\n", resultList));
        if (avinfoList.size() > 0) fileReaderAndWriterMap.writeKeyFile("avinfo", String.join("\n", avinfoList));
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
    }
}