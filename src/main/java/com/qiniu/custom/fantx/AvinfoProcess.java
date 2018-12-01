package com.qiniu.custom.fantx;

import com.qiniu.common.FileMap;
import com.qiniu.model.media.Avinfo;
import com.qiniu.model.media.VideoStream;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.ObjectUtils;
import com.qiniu.util.UrlSafeBase64;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class AvinfoProcess implements ILineProcess<Map<String, String>>, Cloneable {

    private String saveBucket;
    private String processName;
    protected String resultFileDir;
    private int resultFileIndex;
    private FileMap fileMap;
    private String mp4Fop1080 = "avthumb/mp4/s/1920x1080/autoscale/1|saveas/";
    private String mp4Fop720 = "avthumb/mp4/s/1280x720/autoscale/1|saveas/";
    private String mp4Fop480 = "avthumb/mp4/s/640x480/autoscale/1|saveas/";
    private String m3u8Copy = "avthumb/m3u8/vcodec/copy/acodec/copy|saveas/";

    private void initBaseParams(String saveBucket) {
        this.processName = "avthumb";
        this.saveBucket = saveBucket;
    }

    public AvinfoProcess(String saveBucket, String resultFileDir) {
        initBaseParams(saveBucket);
        this.resultFileDir = resultFileDir;
        this.fileMap = new FileMap();
    }

    public AvinfoProcess(String saveBucket, String resultFileDir, int resultFileIndex)
            throws IOException {
        this(saveBucket, resultFileDir);
        this.resultFileIndex = resultFileIndex;
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public AvinfoProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        AvinfoProcess avinfoProcess = (AvinfoProcess)super.clone();
        avinfoProcess.resultFileIndex = resultFileIndex;
        avinfoProcess.fileMap = new FileMap();
        try {
            avinfoProcess.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return avinfoProcess;
    }

    public void setBatch(boolean batch) {}

    public void setRetryCount(int retryCount) {}

    public String getProcessName() {
        return this.processName;
    }

    public String getInfo() {
        return "";
    }

    // mp4 retry
    public void processLine(List<Map<String, String>> lineList) {

        lineList = lineList == null ? null : lineList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (lineList == null || lineList.size() == 0) return;

        processLine1(lineList);
//        processLine2(lineList);
//        processLine3(lineList);

    }

    private String generateCopyLine(String key, int width) {
        String keySuffix;
        if (width > 1280) keySuffix = "F1080";
        else if (width > 1000) keySuffix = "F720";
        else keySuffix = "F480";
        String srcCopy = "/copy/" + UrlSafeBase64.encodeToString(saveBucket + ":" + key) + "/";
        String copyKey = ObjectUtils.addSuffixKeepExt(key, keySuffix);
        String copySaveAs = UrlSafeBase64.encodeToString(saveBucket + ":" + copyKey);
        return copyKey + "\t" + key + "\t" + srcCopy + copySaveAs;
    }

    private String generateMp4FopLine(String key, int width, String other) {
        String keySuffix;
        String fop;
        if (width > 1280) {
            keySuffix = "F1080";
            fop = mp4Fop1080;
        }
        else if (width > 1000) {
            keySuffix = "F720";
            fop = mp4Fop720;
        }
        else {
            keySuffix = "F480";
            fop = mp4Fop480;
        }
        String mp4Key = ObjectUtils.addSuffixWithExt(key, keySuffix, "mp4");
        String mp41080SaveAs = UrlSafeBase64.encodeToString(saveBucket + ":" + mp4Key);
        return mp4Key + "\t" + key + "\t" + fop + mp41080SaveAs + other;
    }

    public void processLine1(List<Map<String, String>> lineList) {

        List<String> copyList = new ArrayList<>();
        List<String> mp4FopList = new ArrayList<>();
        List<String> m3u8FopList = new ArrayList<>();

        for (Map<String, String> line : lineList) {
            String key = line.get("0");

            String mp4Key720 = ObjectUtils.addSuffixKeepExt(key, "F720");
            String mp4Key480 = ObjectUtils.addSuffixKeepExt(key, "F480");
            String m3u8Key1080 = ObjectUtils.addSuffixWithExt(key, "F1080", "m3u8");
            String m3u8Key720 = ObjectUtils.addSuffixWithExt(key, "F720", "m3u8");
            String m3u8Key480 = ObjectUtils.addSuffixWithExt(key, "F480", "m3u8");

            String mp4720SaveAs = UrlSafeBase64.encodeToString(saveBucket + ":" + mp4Key720);
            String mp4480SaveAs = UrlSafeBase64.encodeToString(saveBucket + ":" + mp4Key480);
            String m3u81080SaveAs = UrlSafeBase64.encodeToString(saveBucket + ":" + m3u8Key1080);
            String m3u8720SaveAs = UrlSafeBase64.encodeToString(saveBucket + ":" + m3u8Key720);
            String m3u8480SaveAs = UrlSafeBase64.encodeToString(saveBucket + ":" + m3u8Key480);

            try {
                Avinfo avinfo = JsonConvertUtils.fromJson(line.get("1"), Avinfo.class);
                double duration = Double.valueOf(avinfo.getFormat().duration);
                long size = Long.valueOf(avinfo.getFormat().size);
                String other = "\t" + duration + "\t" + size;
                VideoStream videoStream = avinfo.getVideoStream();
                if (videoStream == null) {
                    throw new Exception("videoStream is null");
                }
                int width = videoStream.width;

                if (key.endsWith(".mp4") || key.endsWith(".MP4")) copyList.add(generateCopyLine(key, width));
                else mp4FopList.add(generateMp4FopLine(key, width, other));
                if (width > 1280) {
                    mp4FopList.add(mp4Key720 + "\t" + key + "\t" + mp4Fop720 + mp4720SaveAs + other);
                    mp4FopList.add(mp4Key480 + "\t" + key + "\t" + mp4Fop480 + mp4480SaveAs + other);
                    m3u8FopList.add(m3u8Key1080 + "\t" + key + "\t" + m3u8Copy + m3u81080SaveAs + other);
                    m3u8FopList.add(m3u8Key720 + "\t" + mp4Key720 + "\t" + m3u8Copy + m3u8720SaveAs + other);
                    m3u8FopList.add(m3u8Key480 + "\t" + mp4Key480 + "\t" + m3u8Copy + m3u8480SaveAs + other);
                } else if (width > 1000) {
                    mp4FopList.add(mp4Key480 + "\t" + mp4Fop480 + mp4480SaveAs + other);
                    m3u8FopList.add(m3u8Key720 + "\t" + key + "\t" + m3u8Copy + m3u8720SaveAs + other);
                    m3u8FopList.add(m3u8Key480 + "\t" + mp4Key480 + "\t" + m3u8Copy + m3u8480SaveAs + other);
                } else {
                    m3u8FopList.add(m3u8Key480 + "\t" + key + "\t" + m3u8Copy + m3u8480SaveAs + other);
                }
            } catch (Exception e) {
                fileMap.writeErrorOrNull(e.getMessage() + "\t" + getInfo() + "\t" + line.toString());
            }
        }
        if (copyList.size() > 0) fileMap.writeKeyFile("tocopy" + resultFileIndex, String.join("\n", copyList));
        if (mp4FopList.size() > 0) fileMap.writeKeyFile("tomp4" + resultFileIndex, String.join("\n", mp4FopList));
        if (m3u8FopList.size() > 0) fileMap.writeKeyFile("tom3u8" + resultFileIndex, String.join("\n", m3u8FopList));
    }

    // mp4 retry
    public void processLine2(List<Map<String, String>> lineList) {

        List<String> mp4FopList = new ArrayList<>();

        for (Map<String, String> line : lineList) {
            try {
                String toKey = line.get("0");
                String srcKey = line.get("1");
                String mp4Fop720 = srcKey + "\t" + "avthumb/mp4/s/1280x720/autoscale/1|saveas/";
                String mp4Fop480 = srcKey + "\t" + "avthumb/mp4/s/640x480/autoscale/1|saveas/";

                if (toKey.contains("F480")) {
                    mp4FopList.add(toKey + "\t" + srcKey + "\t" + mp4Fop720 + UrlSafeBase64.encodeToString(saveBucket + ":" + toKey));
                } else if (toKey.contains("F720")) {
                    mp4FopList.add(toKey + "\t" + srcKey + "\t" + mp4Fop480 + UrlSafeBase64.encodeToString(saveBucket + ":" + toKey));
                }
            } catch (Exception e) {
                fileMap.writeErrorOrNull(e.getMessage() + "\t" + getInfo() + "\t" + line.toString());
            }
        }
        if (mp4FopList.size() > 0) fileMap.writeKeyFile("tomp4" + resultFileIndex, String.join("\n", mp4FopList));
    }

    // mp4 retry
    public void processLine3(List<Map<String, String>> lineList) {

        List<String> mp4FopList = new ArrayList<>();

        for (Map<String, String> line : lineList) {
            try {
                String toKey = line.get("0");
                String srcKey = line.get("1");
                String mp4Fop720 = srcKey + "\t" + "avthumb/mp4/s/1280x720/autoscale/1|saveas/";
                String mp4Fop480 = srcKey + "\t" + "avthumb/mp4/s/640x480/autoscale/1|saveas/";

                if (toKey.contains("F480")) {
                    mp4FopList.add(toKey + "\t" + srcKey + "\t" + mp4Fop720 + UrlSafeBase64.encodeToString(saveBucket + ":" + toKey));
                } else if (toKey.contains("F720")) {
                    mp4FopList.add(toKey + "\t" + srcKey + "\t" + mp4Fop480 + UrlSafeBase64.encodeToString(saveBucket + ":" + toKey));
                }
            } catch (Exception e) {
                fileMap.writeErrorOrNull(e.getMessage() + "\t" + getInfo() + "\t" + line.toString());
            }
        }
        if (mp4FopList.size() > 0) fileMap.writeKeyFile("tomp4" + resultFileIndex, String.join("\n", mp4FopList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
