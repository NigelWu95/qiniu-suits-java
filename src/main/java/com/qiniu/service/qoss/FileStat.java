package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FileStat extends OperationBase implements ILineProcess<FileInfo>, Cloneable {

    private String processName;

    private void initBaseParams() {
        this.processName = "stat";
    }

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultFileDir) {
        super(auth, configuration, bucket, resultFileDir);
        initBaseParams();
    }

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultFileDir, int resultFileIndex)
            throws IOException {
        this(auth, configuration, bucket, resultFileDir);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public FileStat getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        FileStat fileStat = (FileStat)super.clone();
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
        return "";
    }

    @Override
    protected Response getResponse(FileInfo fileInfo) {
        return null;
    }

    public FileInfo statWithRetry(FileInfo fileInfo, int retryCount) throws QiniuException {

        FileInfo stat = null;
        try {
            stat = bucketManager.stat(bucket, fileInfo.key);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    stat = bucketManager.stat(bucket, fileInfo.key);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return stat;
    }

    synchronized protected BatchOperations getOperations(List<FileInfo> fileInfoList) {
        return null;
    }

    @Override
    public void processLine(List<FileInfo> fileInfoList) throws QiniuException {

        fileInfoList = fileInfoList == null ? null : fileInfoList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (fileInfoList == null || fileInfoList.size() == 0) return;
        List<String> resultList = new ArrayList<>();
        for (FileInfo fileInfo : fileInfoList) {
            try {
                FileInfo stat = statWithRetry(fileInfo, retryCount);
                if (stat != null) resultList.add(fileInfo.key + "\t" + JsonConvertUtils.toJson(stat));
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
