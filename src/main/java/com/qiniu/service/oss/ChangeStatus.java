package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class ChangeStatus extends OperationBase implements IOssFileProcess, Cloneable {

    private int status;

    private void initOwnParams(int status) {
        this.status = status;
    }

    public ChangeStatus(Auth auth, Configuration configuration, String bucket, int status, String resultFileDir,
                        String processName, int resultFileIndex) throws IOException {
        super(auth, configuration, bucket, resultFileDir, processName, resultFileIndex);
        initOwnParams(status);
    }

    public ChangeStatus(Auth auth, Configuration configuration, String bucket, int status, String resultFileDir,
                        String processName) {
        super(auth, configuration, bucket, resultFileDir, processName);
        initOwnParams(status);
    }

    public ChangeStatus getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        ChangeStatus changeStatus = (ChangeStatus)super.clone();
        changeStatus.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            changeStatus.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return changeStatus;
    }

    protected Response getResponse(FileInfo fileInfo) throws QiniuException {
        return bucketManager.changeStatus(bucket, fileInfo.key, status);
    }

    synchronized protected BatchOperations getOperations(List<FileInfo> fileInfoList){
        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.key).collect(Collectors.toList());
        return batchOperations.addChangeStatusOps(bucket, status, keyList.toArray(new String[]{}));
    }

    protected String getInfo() {
        return bucket + "\t" + status;
    }
}
