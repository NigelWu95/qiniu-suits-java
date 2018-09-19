package com.qiniu.service.impl;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.http.Client;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.auvideo.VideoTS;
import com.qiniu.service.oss.ChangeStatusProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ChangeFileStatusProcess implements IOssFileProcess {

    private ChangeStatusProcessor changeStatusProcessor;
    private FileReaderAndWriterMap targetFileReaderAndWriterMap;
    private M3U8Manager m3u8Manager;

    public ChangeFileStatusProcess(QiniuAuth auth, Client client, FileReaderAndWriterMap targetFileReaderAndWriterMap) throws QiniuSuitsException {
        this.changeStatusProcessor = ChangeStatusProcessor.getChangeStatusProcessor(auth, client);
        this.targetFileReaderAndWriterMap = targetFileReaderAndWriterMap;
    }

    public ChangeFileStatusProcess(QiniuAuth auth, Client client, FileReaderAndWriterMap targetFileReaderAndWriterMap, M3U8Manager m3u8Manager) throws QiniuSuitsException {
        this.changeStatusProcessor = ChangeStatusProcessor.getChangeStatusProcessor(auth, client);
        this.targetFileReaderAndWriterMap = targetFileReaderAndWriterMap;
        this.m3u8Manager = m3u8Manager;
    }

    private void changeStatusResult(String bucket, String key, short status) {
        try {
            String bucketCopyResult = changeStatusProcessor.doStatusChange(bucket, key, status);
            targetFileReaderAndWriterMap.writeSuccess(bucketCopyResult);
        } catch (QiniuSuitsException e) {
            targetFileReaderAndWriterMap.writeErrorAndNull(e.toString() + "\t" + bucket + "\t" + key + "\t" + status);
        }
    }

    public void processKey(String bucket, String key, short status) {
        changeStatusResult(bucket, key, status);
    }

    public void processKey(String bucket, String rootUrl, String key, short status, String format) {
        changeStatusResult(bucket, key, status);

        if (Arrays.asList("hls", "HLS", "m3u8", "M3U8").contains(format)) {
            changeTSByM3U8(bucket, rootUrl, key, status);
        }
    }

    public void changeTSByM3U8(String bucket, String rootUrl, String key, short status) {
        List<VideoTS> videoTSList = new ArrayList<>();

        try {
            videoTSList = m3u8Manager.getVideoTSListByFile(rootUrl, key);
        } catch (IOException ioException) {
            targetFileReaderAndWriterMap.writeOther("list ts failed: " + key);
        }

        for (VideoTS videoTS : videoTSList) {
            processKey(bucket, videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1], status);
        }
    }

    public void close() {
        if (changeStatusProcessor != null) {
            changeStatusProcessor.closeClient();
        }
    }
}