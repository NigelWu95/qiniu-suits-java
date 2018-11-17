package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.StorageType;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;

public class ChangeType extends OperationBase implements IOssFileProcess, Cloneable {

    private int type;

    public ChangeType(Auth auth, Configuration configuration, String bucket, int type, String resultFileDir,
                      String processName, int resultFileIndex) throws IOException {
        super(auth, configuration, bucket, resultFileDir, processName, resultFileIndex);
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

    protected Response getResponse(String key) throws QiniuException {
        StorageType storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;
        return bucketManager.changeType(bucket, key, storageType);
    }

    protected BatchOperations getOperations(List<String> keys){
        return batchOperations.addChangeTypeOps(bucket, type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY,
                keys.toArray(new String[]{}));
    }

    protected String getInfo() {
        return bucket + "\t" + type;
    }
}
