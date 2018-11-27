package com.qiniu.custom.fantx;

import com.qiniu.common.FileMap;
import com.qiniu.model.media.Avinfo;
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

    private String domain;
    private String saveBucket;
    private String processName;
    protected String resultFileDir;
    private int resultFileIndex;
    private FileMap fileMap;

    private void initBaseParams(String domain, String saveBucket) {
        this.processName = "avinfo";
        this.domain = domain;
        this.saveBucket = saveBucket;
    }

    public AvinfoProcess(String domain, String saveBucket, String resultFileDir) {
        initBaseParams(domain, saveBucket);
        this.resultFileDir = resultFileDir;
        this.fileMap = new FileMap();
    }

    public AvinfoProcess(String domain, String saveBucket, String resultFileDir, int resultFileIndex)
            throws IOException {
        this(domain, saveBucket, resultFileDir);
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
        return domain;
    }

    public void processLine(List<Map<String, String>> lineList) {

        lineList = lineList == null ? null : lineList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (lineList == null || lineList.size() == 0) return;
        List<String> copyList = new ArrayList<>();
        List<String> mp4FopList = new ArrayList<>();
        List<String> m3u8FopList = new ArrayList<>();

        for (Map<String, String> line : lineList) {
            String key = line.get("0");
            String srcCopy = key + "\t" + "/copy/" + UrlSafeBase64.encodeToString(saveBucket + ":" + key) + "/";
            String mp4Fop720 = key + "\t" + "avthumb/mp4/s/1280x720/autoscale/1|saveas/";
            String mp4Fop480 = key + "\t" + "avthumb/mp4/s/640x480/autoscale/1|saveas/";
            String m3u8Copy = "avthumb/m3u8/vcodec/copy/acodec/copy|saveas/";
            String mp4Key720 = ObjectUtils.addSuffixKeepExt(key, "F720");
            String mp4Key480 = ObjectUtils.addSuffixKeepExt(key, "F480");
            String m3u8Key1080 = ObjectUtils.addSuffixWithExt(key, "F1080", "m3u8");
            String m3u8Key720 = ObjectUtils.addSuffixWithExt(key, "F720", "m3u8");
            String m3u8Key480 = ObjectUtils.addSuffixWithExt(key, "F480", "m3u8");

            try {
                Avinfo avinfo = JsonConvertUtils.fromJson(line.get("1"), Avinfo.class);
                double duration = Double.valueOf(avinfo.getFormat().duration);
                long size = Long.valueOf(avinfo.getFormat().size);
                String other = "\t" + duration + "\t" + size;
                int width = avinfo.getVideoStream().width;
                if (width > 1280) {
                    String copyKey1080 = ObjectUtils.addSuffixKeepExt(key, "F1080");
                    copyList.add(srcCopy + UrlSafeBase64.encodeToString(saveBucket + ":" + copyKey1080));
                    mp4FopList.add(mp4Fop720 + UrlSafeBase64.encodeToString(saveBucket + ":" + mp4Key720 + other));
                    mp4FopList.add(mp4Fop480 + UrlSafeBase64.encodeToString(saveBucket + ":" + mp4Key480) + other);
                    m3u8FopList.add(copyKey1080 + "\t" + m3u8Copy + UrlSafeBase64.encodeToString(saveBucket + ":" + m3u8Key1080) + other);
                    m3u8FopList.add(mp4Key720 + "\t" + m3u8Copy + UrlSafeBase64.encodeToString(saveBucket + ":" + m3u8Key720) + other);
                    m3u8FopList.add(mp4Key480 + "\t" + m3u8Copy + UrlSafeBase64.encodeToString(saveBucket + ":" + m3u8Key480) + other);
                } else if (width > 1000) {
                    String copyKey720 = ObjectUtils.addSuffixKeepExt(key, "F720");
                    copyList.add(srcCopy + UrlSafeBase64.encodeToString(saveBucket + ":" + copyKey720));
                    mp4FopList.add(mp4Fop480 + UrlSafeBase64.encodeToString(saveBucket + ":" + mp4Key480) + other);
                    m3u8FopList.add(mp4Key720 + "\t" + m3u8Copy + UrlSafeBase64.encodeToString(saveBucket + ":" + m3u8Key720) + other);
                    m3u8FopList.add(mp4Key480 + "\t" + m3u8Copy + UrlSafeBase64.encodeToString(saveBucket + ":" + m3u8Key480) + other);
                } else {
                    String copyKey480 = ObjectUtils.addSuffixKeepExt(key, "F480");
                    copyList.add(srcCopy + UrlSafeBase64.encodeToString(saveBucket + ":" + copyKey480));
                    m3u8FopList.add(copyKey480 + "\t" + m3u8Copy + UrlSafeBase64.encodeToString(saveBucket + ":" + m3u8Key480) + other);
                }
            } catch (Exception e) {
                fileMap.writeErrorOrNull(e.getMessage() + "\t" + getInfo() + "\t" + key);
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
