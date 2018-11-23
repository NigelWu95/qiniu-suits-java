package com.qiniu.service.media;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.model.media.Avinfo;
import com.qiniu.service.interfaces.IQossProcess;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class QueryAvinfo implements IQossProcess, Cloneable {

    private String domain;
    private MediaManager mediaManager;
    private String processName;
    private int retryCount = 3;
    protected String resultFileDir;
    private int resultFileIndex;
    private FileReaderAndWriterMap fileReaderAndWriterMap;

    private void initBaseParams(String domain) {
        this.processName = "avinfo";
        this.domain = domain;
    }

    public QueryAvinfo(String domain, String resultFileDir) {
        initBaseParams(domain);
        this.resultFileDir = resultFileDir;
        this.mediaManager = new MediaManager();
        this.fileReaderAndWriterMap = new FileReaderAndWriterMap();
    }

    public QueryAvinfo(String domain, String resultFileDir, int resultFileIndex) throws IOException {
        this(domain, resultFileDir);
        this.resultFileIndex = resultFileIndex;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public QueryAvinfo getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        QueryAvinfo queryAvinfo = (QueryAvinfo)super.clone();
        queryAvinfo.resultFileIndex = resultFileIndex;
        queryAvinfo.mediaManager = new MediaManager();
        queryAvinfo.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            queryAvinfo.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
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

    public void processFile(List<FileInfo> fileInfoList, int retryCount) throws QiniuException {

        fileInfoList = fileInfoList == null ? null : fileInfoList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (fileInfoList == null || fileInfoList.size() == 0) return;

        List<String> resultList = new ArrayList<>();
        List<String> avinfoList = new ArrayList<>();
        for (FileInfo fileInfo : fileInfoList) {
            try {
                Avinfo avinfo = singleWithRetry(fileInfo, retryCount);
                avinfoList.add(fileInfo.key + "\t" + mediaManager.getCurrentAvinfoJson());
                resultList.addAll(ConvertFopCommand(fileInfo.key, avinfo));
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileReaderAndWriterMap, processName, getInfo() +
                        "\t" + fileInfo.key);
            }
        }
        if (resultList.size() > 0) fileReaderAndWriterMap.writeSuccess(String.join("\n", resultList));
        if (avinfoList.size() > 0) fileReaderAndWriterMap.writeKeyFile("avinfo" + resultFileIndex,
                String.join("\n", avinfoList));
    }

    public void processFile(List<String> avinfoList) throws QiniuException {

        avinfoList = avinfoList == null ? null : avinfoList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (avinfoList == null || avinfoList.size() == 0) return;

        List<String> resultList = new ArrayList<>();
        for (String avinfoLine : avinfoList) {
            String[] items = avinfoLine.split("\t");
            try {
                Avinfo avinfo = JsonConvertUtils.fromJson(items[1], Avinfo.class);
                resultList = ConvertFopCommand(items[0], avinfo);
            } catch (Exception e) {
                HttpResponseUtils.processException(new QiniuException(e), fileReaderAndWriterMap, processName,
                        getInfo() + "\t" + items[0]);
            }
        }
        if (resultList.size() > 0) fileReaderAndWriterMap.writeSuccess(String.join("\n", resultList));
    }

    public List<String> ConvertFopCommand(String key, Avinfo avinfo) {
        List<String> resultList = new ArrayList<>();
        int width = avinfo.getVideoStream().width;
        String s = width + "x" + avinfo.getVideoStream().height;
        if (width > 1280) {
            String mp4Fop = key + "\t" + "avthumb/mp4/s/" + s + "/saveas/";
            String mp4Key = ObjectUtils.addSuffixKeepExt(key, "F1080");
            resultList.add(mp4Fop + UrlSafeBase64.encodeToString("fantasy-tv-avthumb:" + mp4Key));
            String m3u8Fop = mp4Key + "\t" + "avthumb/m3u8/s/" + s + "/vcodec/copy/acodec/copy|saveas/";
            String m3u8Key = ObjectUtils.addSuffixWithExt(key, "F1080", "m3u8");
            resultList.add(m3u8Fop + UrlSafeBase64.encodeToString("fantasy-tv-avthumb:" + m3u8Key));
        } else if (width > 1000) {
            String mp4Fop = key + "\t" + "avthumb/mp4/s/" + s + "/saveas/";
            String mp4Key = ObjectUtils.addSuffixKeepExt(key, "F720");
            resultList.add(mp4Fop + UrlSafeBase64.encodeToString("fantasy-tv-avthumb:" + mp4Key));
            String m3u8Fop = mp4Key + "\t" + "avthumb/m3u8/s/" + s + "/vcodec/copy/acodec/copy|saveas/";
            String m3u8Key = ObjectUtils.addSuffixWithExt(key, "F720", "m3u8");
            resultList.add(m3u8Fop + UrlSafeBase64.encodeToString("fantasy-tv-avthumb:" + m3u8Key));
        } else if (width > 640) {
            String mp4Fop = key + "\t" + "avthumb/mp4/s/640x480|saveas/";
            String mp4Key = ObjectUtils.addSuffixKeepExt(key, "F480");
            resultList.add(mp4Fop + UrlSafeBase64.encodeToString("fantasy-tv-avthumb:" + mp4Key));
            String m3u8Fop = mp4Key + "\t" + "avthumb/mp4/s/640x480/vcodec/copy/acodec/copy|saveas/";
            String m3u8Key = ObjectUtils.addSuffixWithExt(key, "F480", "m3u8");
            resultList.add(m3u8Fop + UrlSafeBase64.encodeToString("fantasy-tv-avthumb:" + m3u8Key));
        } else {
            String mp4Fop = key + "\t" + "avthumb/mp4/s/" + s + "/saveas/";
            String mp4Key = ObjectUtils.addSuffixKeepExt(key, "F480");
            resultList.add(mp4Fop + UrlSafeBase64.encodeToString("fantasy-tv-avthumb:" + mp4Key));
            String m3u8Fop = mp4Key + "\t" + "avthumb/m3u8/s/" + s + "/vcodec/copy/acodec/copy|saveas/";
            String m3u8Key = ObjectUtils.addSuffixWithExt(key, "F480", "m3u8");
            resultList.add(m3u8Fop + UrlSafeBase64.encodeToString("fantasy-tv-avthumb:" + m3u8Key));
        }
        return resultList;
    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
    }
}