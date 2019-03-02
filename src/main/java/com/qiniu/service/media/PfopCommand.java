package com.qiniu.service.media;

import com.qiniu.common.QiniuException;
import com.qiniu.model.media.Avinfo;
import com.qiniu.model.media.VideoStream;
import com.qiniu.persistence.FileMap;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.FileNameUtils;
import com.qiniu.util.UrlSafeBase64;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PfopCommand implements ILineProcess<Map<String, String>>, Cloneable {

    private String processName;
    private MediaManager mediaManager;
    private String resultPath;
    private String resultTag;
    private int resultIndex;
    private FileMap fileMap;
    private String bucket;
    private String mp4Fop1080 = "avthumb/mp4/s/1920x1080/autoscale/1|saveas/";
    private String mp4Fop720 = "avthumb/mp4/s/1280x720/autoscale/1|saveas/";
    private String mp4Fop480 = "avthumb/mp4/s/640x480/autoscale/1|saveas/";
    private String m3u8Copy = "avthumb/m3u8/vcodec/copy/acodec/copy|saveas/";

    public PfopCommand(String bucket, String resultPath, int resultIndex) throws IOException {
        this.processName = "pfopcmd";
        this.mediaManager = new MediaManager();
        this.bucket = bucket;
        this.resultPath = resultPath;
        this.resultTag = "";
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap(resultPath, processName, String.valueOf(resultIndex));
        this.fileMap.initDefaultWriters();
    }

    public PfopCommand(String bucket, String resultPath) throws IOException {
        this(bucket, resultPath, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setResultTag(String resultTag) {
        this.resultTag = resultTag == null ? "" : resultTag;
    }

    public PfopCommand clone() throws CloneNotSupportedException {
        PfopCommand pfopCommand = (PfopCommand)super.clone();
        pfopCommand.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(++resultIndex));
        try {
            pfopCommand.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return pfopCommand;
    }

    private String generateCopyLine(String key, int width) throws QiniuException {
        String keySuffix;
        if (width > 1280) keySuffix = "F1080";
        else if (width > 1000) keySuffix = "F720";
        else keySuffix = "F480";
        String copyKey = FileNameUtils.addSuffixKeepExt(key, keySuffix);
        return key + "\t" + copyKey;
    }

    private String generateMp4FopLine(String key, int width) throws QiniuException {
        String keySuffix;
        String fop;
        if (width > 1280) {
            keySuffix = "F1080";
            fop = mp4Fop1080;
        } else if (width > 1000) {
            keySuffix = "F720";
            fop = mp4Fop720;
        } else {
            keySuffix = "F480";
            fop = mp4Fop480;
        }
        String mp4Key = FileNameUtils.addSuffixWithExt(key, keySuffix, "mp4");
        return generateFopLine(key, fop, mp4Key);
    }

    private String generateFopLine(String key, String fop, String toKey) {
        String saveAsEntry = UrlSafeBase64.encodeToString(bucket + ":" + toKey);
        return key + "\t" + fop + saveAsEntry + "\t" + toKey;
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        List<String> copyList = new ArrayList<>();
        List<String> mp4FopList = new ArrayList<>();
        List<String> m3u8FopList = new ArrayList<>();
        String key;
        String info;

        for (Map<String, String> line : lineList) {
            key = line.get("key");
            info = line.get("avinfo");
            if (key == null || "".equals(key) || info == null || "".equals(info))
                throw new IOException("target value is empty.");
            try {
                Avinfo avinfo = mediaManager.getAvinfoByJson(info);
                double duration = Double.valueOf(avinfo.getFormat().duration);
                long size = Long.valueOf(avinfo.getFormat().size);
                String other = "\t" + duration + "\t" + size;
                VideoStream videoStream = avinfo.getVideoStream();
                if (videoStream == null) {
                    throw new Exception("videoStream is null");
                }
                int width = videoStream.width;

                String mp4Key1080 = FileNameUtils.addSuffixKeepExt(key, "F1080");
                String mp4Key720 = FileNameUtils.addSuffixKeepExt(key, "F720");
                String mp4Key480 = FileNameUtils.addSuffixKeepExt(key, "F480");
                String m3u8Key1080 = FileNameUtils.addSuffixWithExt(key, "F1080", "m3u8");
                String m3u8Key720 = FileNameUtils.addSuffixWithExt(key, "F720", "m3u8");
                String m3u8Key480 = FileNameUtils.addSuffixWithExt(key, "F480", "m3u8");
                if (key.endsWith(".mp4") || key.endsWith(".MP4")) {
                    // 原文件如果为 mp4，则直接 copy 改名
                    copyList.add(generateCopyLine(key, width));
                } else {
                    // 原文件如果不为 mp4，经过转码后产生规则命名的 mp4 文件
                    mp4Key1080 = FileNameUtils.addSuffixWithExt(key, "F1080", "mp4");
                    mp4Key720 = FileNameUtils.addSuffixWithExt(key, "F720", "mp4");
                    mp4Key480 = FileNameUtils.addSuffixWithExt(key, "F480", "mp4");
                    mp4FopList.add(generateMp4FopLine(key, width) + other);
                }

                if (width > 1280) {
                    mp4FopList.add(generateFopLine(key, mp4Fop720, mp4Key720) + other);
                    mp4FopList.add(generateFopLine(key, mp4Fop480, mp4Key480) + other);
                    m3u8FopList.add(generateFopLine(mp4Key1080, m3u8Copy, m3u8Key1080) + other);
                    m3u8FopList.add(generateFopLine(mp4Key720, m3u8Copy, m3u8Key720) + other);
                    m3u8FopList.add(generateFopLine(mp4Key480, m3u8Copy, m3u8Key480) + other);
                } else if (width > 1000) {
                    mp4FopList.add(generateFopLine(key, mp4Fop480, mp4Key480) + other);
                    m3u8FopList.add(generateFopLine(mp4Key720, m3u8Copy, m3u8Key720) + other);
                    m3u8FopList.add(generateFopLine(mp4Key480, m3u8Copy, m3u8Key480) + other);
                } else {
                    m3u8FopList.add(generateFopLine(mp4Key480, m3u8Copy, m3u8Key480) + other);
                }
            } catch (Exception e) {
                fileMap.writeError(String.valueOf(line) + "\t" + e.getMessage(), false);
            }
        }
        if (copyList.size() > 0)
            fileMap.writeKeyFile("tocopy" + resultIndex, String.join("\n", copyList), false);
        if (mp4FopList.size() > 0)
            fileMap.writeKeyFile("tomp4" + resultIndex, String.join("\n", mp4FopList), false);
        if (m3u8FopList.size() > 0)
            fileMap.writeKeyFile("tom3u8" + resultIndex, String.join("\n", m3u8FopList), false);
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
