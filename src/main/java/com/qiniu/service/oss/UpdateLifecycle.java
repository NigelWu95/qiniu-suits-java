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

public class UpdateLifecycle extends OperationBase implements IOssFileProcess, Cloneable {

    private int days;

    public UpdateLifecycle(Auth auth, Configuration configuration, String bucket, int days, String resultFileDir,
                           String processName, int resultFileIndex) throws IOException {
        super(auth, configuration, bucket, resultFileDir, processName, resultFileIndex);
        this.days = days;
    }

    public UpdateLifecycle(Auth auth, Configuration configuration, String bucket, int days, String resultFileDir,
                           String processName) throws IOException {
        this(auth, configuration, bucket, days, resultFileDir, processName, 0);
    }

    public UpdateLifecycle getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        UpdateLifecycle updateLifecycle = (UpdateLifecycle)super.clone();
        updateLifecycle.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            updateLifecycle.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return updateLifecycle;
    }

    protected Response getResponse(String key) throws QiniuException {
        return bucketManager.deleteAfterDays(bucket, key, days);
    }

    synchronized protected BatchOperations getOperations(List<String> keys){
        return batchOperations.addDeleteAfterDaysOps(bucket, days, keys.toArray(new String[]{}));
    }

    protected String getInfo() {
        return bucket + "\t" + days;
    }
}
