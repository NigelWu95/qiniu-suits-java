package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;

public class ChangeStatus extends OperationBase implements IOssFileProcess, Cloneable {

    private int status;

    public ChangeStatus(Auth auth, Configuration configuration, String bucket, int status, String resultFileDir,
                        String processName, int resultFileIndex) throws IOException {
        super(auth, configuration, bucket, resultFileDir, processName, resultFileIndex);
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

    protected Response getResponse(String key) throws QiniuException {
        return bucketManager.changeStatus(bucket, key, status);
    }

    protected BatchOperations getOperations(List<String> keys){
        return batchOperations.addChangeStatusOps(bucket, status, keys.toArray(new String[]{}));
    }

    protected String getInfo() {
        return bucket + "\t" + status;
    }
}
