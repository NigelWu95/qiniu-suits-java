package com.qiniu.service.qoss;

import com.qiniu.common.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.interfaces.IQossProcess;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FileStat implements IQossProcess, Cloneable {

    private String domain;
    private FileChecker fileChecker;
    private String processName;
    private int retryCount = 3;
    protected String resultFileDir;
    private FileMap fileMap;

    private void initBaseParams(String domain) {
        this.processName = "hash";
        this.domain = domain;
    }

    public FileStat(String domain, String resultFileDir) {
        initBaseParams(domain);
        this.resultFileDir = resultFileDir;
        this.fileChecker = new FileChecker(null);
        this.fileMap = new FileMap();
    }

    public FileStat(String domain, String resultFileDir, int resultFileIndex) throws IOException {
        this(domain, resultFileDir);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public FileStat getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        FileStat fileStat = (FileStat)super.clone();
        fileStat.fileChecker = new FileChecker(null);
        fileStat.fileMap = new FileMap();
        try {
            fileStat.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return fileStat;
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

    public FileInfo singleWithRetry(FileInfo fileInfo, int retryCount) throws QiniuException {

        FileInfo stat = null;
        try {
            stat = fileChecker.getStat(domain, fileInfo.key);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    stat = fileChecker.getStat(domain, fileInfo.key);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return stat;
    }

    public void processFile(List<FileInfo> fileInfoList) throws QiniuException {

        fileInfoList = fileInfoList == null ? null : fileInfoList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (fileInfoList == null || fileInfoList.size() == 0) return;
        List<String> resultList = new ArrayList<>();
        for (FileInfo fileInfo : fileInfoList) {
            try {
                FileInfo stat = singleWithRetry(fileInfo, retryCount);
                if (stat != null) resultList.add(fileInfo.key + "\t" + JsonConvertUtils.toJsonWithoutUrlEscape(stat));
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, processName, getInfo() + "\t" + fileInfo.key);
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
