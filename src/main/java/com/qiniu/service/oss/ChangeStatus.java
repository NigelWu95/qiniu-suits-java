package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ChangeStatus extends OperationBase implements IOssFileProcess, Cloneable {

    private String bucket;
    private int status;

    public ChangeStatus(Auth auth, Configuration configuration, String bucket, int status, String resultFileDir,
                        String processName, int resultFileIndex) throws IOException {
        super(auth, configuration, resultFileDir, processName, resultFileIndex);
        this.bucket = bucket;
        this.status = status;
    }

    public ChangeStatus(Auth auth, Configuration configuration, String bucket, int status, String resultFileDir,
                        String processName) throws IOException {
        this(auth, configuration, bucket, status, resultFileDir, processName, 0);
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

    public String getProcessName() {
        return this.processName;
    }

    public Response singleWithRetry(String key, int retryCount) throws QiniuException {

        Response response = null;
        try {
            response = bucketManager.changeStatus(bucket, key, status);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    response = bucketManager.changeStatus(bucket, key, status);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    protected BatchOperations getOperations(List<String> keys){
        return batchOperations.addChangeStatusOps(bucket, status, keys.toArray(new String[]{}));
    }

    protected String getInfo() {
        return bucket + "\t" + status;
    }
}
