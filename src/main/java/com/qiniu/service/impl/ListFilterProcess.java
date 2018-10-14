package com.qiniu.service.impl;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.JSONConvertUtils;

import java.io.IOException;
import java.util.Arrays;

public class ListFilterProcess implements IOssFileProcess, Cloneable {

    private String resultFileDir;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();

    public ListFilterProcess(String resultFileDir) throws IOException {
        this.resultFileDir = resultFileDir;
        this.fileReaderAndWriterMap.addWriter(resultFileDir, "filter_result");
    }

    public ListFilterProcess clone() throws CloneNotSupportedException {
        ListFilterProcess listFilterProcess = (ListFilterProcess)super.clone();
        listFilterProcess.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            listFilterProcess.fileReaderAndWriterMap.addWriter(resultFileDir, "filter_result");
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return listFilterProcess;
    }

    public QiniuException qiniuException() {
        return null;
    }

    public boolean filterKeyPrefix(FileInfo fileInfo, String[] keyPrefix) {

        if (fileInfo == null || keyPrefix == null) return false;
        else return Arrays.asList(keyPrefix).parallelStream().anyMatch(prefix -> fileInfo.key.startsWith(prefix));
    }

    public boolean filterKeySuffix(FileInfo fileInfo, String[] keySuffix) {

        if (fileInfo == null || keySuffix == null) return false;
        else return Arrays.asList(keySuffix).parallelStream().anyMatch(suffix -> fileInfo.key.endsWith(suffix));
    }

    public boolean filterKeyRegex(FileInfo fileInfo, String[] keyRegex) {

        if (fileInfo == null || keyRegex == null) return false;
        else return Arrays.asList(keyRegex).parallelStream().anyMatch(regex -> fileInfo.key.matches(regex));
    }

    public boolean filterFileSize(FileInfo fileInfo, long[] fileSize) {

        if (fileInfo == null || fileSize == null) return false;
        else return fileSize[0] < fileInfo.fsize && fileInfo.fsize <= fileSize[1];
    }

    public boolean filterPutTime(FileInfo fileInfo, long[] putTime) {

        if (fileInfo == null || putTime == null) return false;
        else return putTime[0] < fileInfo.putTime && fileInfo.putTime <= putTime[1];
    }

    public boolean filterMimeType(FileInfo fileInfo, String[] mimeType) {

        if (fileInfo == null || mimeType == null) return false;
        else return Arrays.asList(mimeType).parallelStream().anyMatch(mime -> fileInfo.mimeType.contains(mime));
    }

    public boolean filterType(FileInfo fileInfo, short type) {

        if (fileInfo == null || type < 0) return false;
        else return (fileInfo.type == type);
    }

    public void processFile(String fileInfoStr, int retryCount, boolean batch) {

        FileInfo fileInfo = JSONConvertUtils.fromJson(fileInfoStr, FileInfo.class);
        String[] keySuffix = new String[]{".m3u8"};
        String[] mimeType = new String[]{"video", "application/x-mpegurl"};
        String[] keyRegex = new String[]{".*_compress_L[\\d].*"};

        boolean filter = filterKeySuffix(fileInfo, keySuffix) || filterMimeType(fileInfo, mimeType);
        boolean antiFilter = filterKeyRegex(fileInfo, keyRegex);

        if (filter && !antiFilter) {
            fileReaderAndWriterMap.writeKeyFile("filter_result", fileInfoStr);
        }
    }

    public void checkBatchProcess(int retryCount) {}

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
    }
}