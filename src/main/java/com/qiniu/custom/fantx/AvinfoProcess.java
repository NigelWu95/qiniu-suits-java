package com.qiniu.custom.fantx;

import com.qiniu.common.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.model.media.Avinfo;
import com.qiniu.service.interfaces.IQossProcess;
import com.qiniu.service.media.MediaManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.ObjectUtils;
import com.qiniu.util.UrlSafeBase64;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AvinfoProcess implements IQossProcess, Cloneable {

    private String domain;
    private MediaManager mediaManager;
    private String processName;
    private int retryCount = 3;
    protected String resultFileDir;
    private int resultFileIndex;
    private FileMap fileMap;

    private void initBaseParams(String domain) {
        this.processName = "avinfo";
        this.domain = domain;
    }

    public AvinfoProcess(String domain, String resultFileDir) {
        initBaseParams(domain);
        this.resultFileDir = resultFileDir;
        this.mediaManager = new MediaManager();
        this.fileMap = new FileMap();
    }

    public AvinfoProcess(String domain, String resultFileDir, int resultFileIndex) throws IOException {
        this(domain, resultFileDir);
        this.resultFileIndex = resultFileIndex;
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public AvinfoProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        AvinfoProcess avinfoProcess = (AvinfoProcess)super.clone();
        avinfoProcess.resultFileIndex = resultFileIndex;
        avinfoProcess.mediaManager = new MediaManager();
        avinfoProcess.fileMap = new FileMap();
        try {
            avinfoProcess.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return avinfoProcess;
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
            avinfo = mediaManager.getAvinfoByJson(fileInfo.hash);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    avinfo = mediaManager.getAvinfoByUrl(domain, fileInfo.key);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return avinfo;
    }

    public void processFile(List<FileInfo> fileInfoList) throws QiniuException {

        fileInfoList = fileInfoList == null ? null : fileInfoList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (fileInfoList == null || fileInfoList.size() == 0) return;
        List<String> copyList = new ArrayList<>();
        List<String> mp4FopList = new ArrayList<>();
        List<String> m3u8FopList = new ArrayList<>();
        for (FileInfo fileInfo : fileInfoList) {
            String srcCopy = fileInfo.key + "\t" + "/copy/" +
                    UrlSafeBase64.encodeToString("fantasy-tv:" + fileInfo.key) + "/";
            String mp4Fop720 = fileInfo.key + "\t" + "avthumb/mp4/s/1280x720/autoscale/1|saveas/";
            String mp4Fop480 = fileInfo.key + "\t" + "avthumb/mp4/s/640x480/autoscale/1|saveas/";
            String m3u8Copy = fileInfo.key + "\t" + "avthumb/m3u8/vcodec/copy/acodec/copy|saveas/";
            String mp4Key720 = ObjectUtils.addSuffixKeepExt(fileInfo.key, "F720");
            String mp4Key480 = ObjectUtils.addSuffixKeepExt(fileInfo.key, "F480");
            String m3u8Key1080 = ObjectUtils.addSuffixWithExt(fileInfo.key, "F1080", "m3u8");
            String m3u8Key720 = ObjectUtils.addSuffixWithExt(fileInfo.key, "F720", "m3u8");
            String m3u8Key480 = ObjectUtils.addSuffixWithExt(fileInfo.key, "F480", "m3u8");
            try {
                Avinfo avinfo = singleWithRetry(fileInfo, retryCount);
                double duration = Double.valueOf(avinfo.getFormat().duration);
                long size = Long.valueOf(avinfo.getFormat().size);
                int width = avinfo.getVideoStream().width;
                if (width > 1280) {
                    String copyKey1080 = ObjectUtils.addSuffixKeepExt(fileInfo.key, "F1080");
                    copyList.add(srcCopy + UrlSafeBase64.encodeToString("fantasy-tv:" + copyKey1080));
                    mp4FopList.add(mp4Fop720 + UrlSafeBase64.encodeToString("fantasy-tv:" + mp4Key720 + "\t" + duration + "\t" + size));
                    mp4FopList.add(mp4Fop480 + UrlSafeBase64.encodeToString("fantasy-tv:" + mp4Key480) + "\t" + duration + "\t" + size);
                    m3u8FopList.add(m3u8Copy + UrlSafeBase64.encodeToString("fantasy-tv:" + m3u8Key1080) + "\t" + duration + "\t" + size);
                    m3u8FopList.add(m3u8Copy + UrlSafeBase64.encodeToString("fantasy-tv:" + m3u8Key720) + "\t" + duration + "\t" + size);
                    m3u8FopList.add(m3u8Copy + UrlSafeBase64.encodeToString("fantasy-tv:" + m3u8Key480) + "\t" + duration + "\t" + size);
                } else if (width > 1000) {
                    String copyKey720 = ObjectUtils.addSuffixKeepExt(fileInfo.key, "F720");
                    copyList.add(srcCopy + UrlSafeBase64.encodeToString("fantasy-tv:" + copyKey720));
                    mp4FopList.add(mp4Fop480 + UrlSafeBase64.encodeToString("fantasy-tv:" + mp4Key480) + "\t" + duration + "\t" + size);
                    m3u8FopList.add(m3u8Copy + UrlSafeBase64.encodeToString("fantasy-tv:" + m3u8Key720) + "\t" + duration + "\t" + size);
                    m3u8FopList.add(m3u8Copy + UrlSafeBase64.encodeToString("fantasy-tv:" + m3u8Key480) + "\t" + duration + "\t" + size);
                } else {
                    String copyKey480 = ObjectUtils.addSuffixKeepExt(fileInfo.key, "F480");
                    copyList.add(srcCopy + UrlSafeBase64.encodeToString("fantasy-tv:" + copyKey480));
                    m3u8FopList.add(m3u8Copy + UrlSafeBase64.encodeToString("fantasy-tv:" + m3u8Key480) + "\t" + duration + "\t" + size);
                }
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, processName, getInfo() + "\t" + fileInfo.key);
            }
        }
        if (copyList.size() > 0) fileMap.writeKeyFile("tocopy" + resultFileIndex, String.join("\n", copyList));
        if (mp4FopList.size() > 0) fileMap.writeKeyFile("tomp4" + resultFileIndex, String.join("\n", mp4FopList));
        if (m3u8FopList.size() > 0) fileMap.writeKeyFile("tom3u8" + resultFileIndex, String.join("\n", m3u8FopList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}