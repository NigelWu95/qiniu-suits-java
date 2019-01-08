package com.qiniu.custom;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.model.media.Avinfo;
import com.qiniu.model.media.VideoStream;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.media.MediaManager;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CAvinfoProcess implements ILineProcess<Map<String, String>>, Cloneable {

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

    public CAvinfoProcess(String bucket, String resultPath, int resultIndex) throws IOException {
        this.processName = "fop";
        this.mediaManager = new MediaManager();
        this.bucket = bucket;
        this.resultPath = resultPath;
        this.resultTag = "";
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap(resultPath, processName, String.valueOf(resultIndex));
        this.fileMap.initDefaultWriters();
    }

    public CAvinfoProcess(String bucket, String resultPath) throws IOException {
        this(bucket, resultPath, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setResultTag(String resultTag) {
        this.resultTag = resultTag == null ? "" : resultTag;
    }

    public CAvinfoProcess clone() throws CloneNotSupportedException {
        CAvinfoProcess avinfoProcess = (CAvinfoProcess)super.clone();
        avinfoProcess.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(++resultIndex));
        try {
            avinfoProcess.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return avinfoProcess;
    }

    private String generateCopyLine(String key, int width) throws QiniuException {
        String keySuffix;
        if (width > 1280) keySuffix = "F1080";
        else if (width > 1000) keySuffix = "F720";
        else keySuffix = "F480";
        String copyKey = FileNameUtils.addSuffixKeepExt(key, keySuffix);
        return key + "\t" + copyKey;
//        String srcCopy = "/copy/" + UrlSafeBase64.encodeToString(bucket + ":" + key) + "/";
//        String copySaveAs = UrlSafeBase64.encodeToString(bucket + ":" + copyKey);
//        return copyKey + "\t" + key + "\t" + srcCopy + copySaveAs;
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
        return generateFopLine(key, mp4Key, fop);
    }

    private String generateFopLine(String key, String toKey, String fop) {
        String saveAsEntry = UrlSafeBase64.encodeToString(bucket + ":" + toKey);
        return toKey + "\t" + key + "\t" + fop + saveAsEntry;
    }

    public void processLine1(List<Map<String, String>> lineList) throws IOException {
        List<String> copyList = new ArrayList<>();
        List<String> mp4FopList = new ArrayList<>();
        List<String> m3u8FopList = new ArrayList<>();

        for (Map<String, String> line : lineList) {
            String key = line.get("0");
            try {
//                Avinfo avinfo = JsonConvertUtils.fromJson(line.get("1"), Avinfo.class);
                Avinfo avinfo = mediaManager.getAvinfoByJson(line.get("1"));
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
                    mp4FopList.add(generateFopLine(key, mp4Key720, mp4Fop720) + other);
                    mp4FopList.add(generateFopLine(key, mp4Key480, mp4Fop480) + other);
                    m3u8FopList.add(generateFopLine(mp4Key1080, m3u8Key1080, m3u8Copy) + other);
                    m3u8FopList.add(generateFopLine(mp4Key720, m3u8Key720, m3u8Copy) + other);
                    m3u8FopList.add(generateFopLine(mp4Key480, m3u8Key480, m3u8Copy) + other);
                } else if (width > 1000) {
                    mp4FopList.add(generateFopLine(key, mp4Key480, mp4Fop480) + other);
                    m3u8FopList.add(generateFopLine(mp4Key720, m3u8Key720, m3u8Copy) + other);
                    m3u8FopList.add(generateFopLine(mp4Key480, m3u8Key480, m3u8Copy) + other);
                } else {
                    m3u8FopList.add(generateFopLine(mp4Key480, m3u8Key480, m3u8Copy) + other);
                }
            } catch (Exception e) {
                fileMap.writeError(line.get("0") + "\t" + line.get("1") + "\t" + e.getMessage());
            }
        }
        if (copyList.size() > 0) fileMap.writeKeyFile("tocopy" + resultIndex, String.join("\n", copyList));
        if (mp4FopList.size() > 0) fileMap.writeKeyFile("tomp4" + resultIndex, String.join("\n", mp4FopList));
        if (m3u8FopList.size() > 0) fileMap.writeKeyFile("tom3u8" + resultIndex, String.join("\n", m3u8FopList));
    }

    // mp4 retry
    public void processLine2(List<Map<String, String>> lineList) throws IOException {
        List<String> mp4FopList = new ArrayList<>();
        for (Map<String, String> line : lineList) {
            try {
                String toKey = line.get("0");
                String toSaveAs = UrlSafeBase64.encodeToString(bucket + ":" + toKey);
                String srcKey = line.get("1");
                if (toKey.contains("F480")) {
                    mp4FopList.add(toKey + "\t" + srcKey + "\t" + mp4Fop720 + toSaveAs);
                } else if (toKey.contains("F720")) {
                    mp4FopList.add(toKey + "\t" + srcKey + "\t" + mp4Fop480 + toSaveAs);
                } else if (toKey.contains("F1080")) {
                    mp4FopList.add(toKey + "\t" + srcKey + "\t" + mp4Fop1080 + toSaveAs);
                }
            } catch (Exception e) {
                fileMap.writeError(line.get("0") + "\t" + line.get("1") + "\t" + e.getMessage());
            }
        }
        if (mp4FopList.size() > 0) fileMap.writeKeyFile("tomp4" + resultIndex, String.join("\n", mp4FopList));
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        processLine1(lineList);
//        processLine2(lineList);
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
