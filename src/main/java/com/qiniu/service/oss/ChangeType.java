package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.StorageType;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ChangeType extends OperationBase implements IOssFileProcess, Cloneable {

    private String bucket;
    private int type;

    public ChangeType(Auth auth, Configuration configuration, String bucket, int type, String resultFileDir,
                      String processName, int resultFileIndex) throws IOException {
        super(auth, configuration, resultFileDir, processName, resultFileIndex);
        this.bucket = bucket;
        this.type = type;
    }

    public ChangeType(Auth auth, Configuration configuration, String bucket, int fileType, String resultFileDir,
                      String processName) throws IOException {
        this(auth, configuration, bucket, fileType, resultFileDir, processName, 0);
    }

    public ChangeType getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        ChangeType changeType = (ChangeType)super.clone();
        changeType.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            changeType.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return changeType;
    }

    public String getProcessName() {
        return this.processName;
    }

    public Response singleWithRetry(String key, int retryCount) throws QiniuException {

        Response response = null;
        StorageType storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;
        try {
            response = bucketManager.changeType(bucket, key, storageType);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    response = bucketManager.changeType(bucket, key, storageType);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    protected BatchOperations getOperations(List<String> keys){
        return batchOperations.addChangeTypeOps(bucket, type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY,
                keys.toArray(new String[]{}));
    }

    protected String getInfo() {
        return bucket + "\t" + type;
    }
}
