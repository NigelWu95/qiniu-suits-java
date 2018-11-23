package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.IQossProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.StorageType;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class ChangeType extends OperationBase implements IQossProcess, Cloneable {

    private int type;

    private void initBaseParams(int type) {
        this.processName = "type";
        this.type = type;
    }

    public ChangeType(Auth auth, Configuration configuration, String bucket, int type, String resultFileDir,
                      int resultFileIndex) throws IOException {
        super(auth, configuration, bucket, resultFileDir);
        initBaseParams(type);
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public ChangeType(Auth auth, Configuration configuration, String bucket, int type, String resultFileDir) {
        super(auth, configuration, bucket, resultFileDir);
        initBaseParams(type);
    }

    public ChangeType getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        ChangeType changeType = (ChangeType)super.clone();
        try {
            changeType.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return changeType;
    }

    public String getInfo() {
        return bucket + "\t" + type;
    }

    protected Response getResponse(FileInfo fileInfo) throws QiniuException {
        StorageType storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;
        return bucketManager.changeType(bucket, fileInfo.key, storageType);
    }

    synchronized protected BatchOperations getOperations(List<FileInfo> fileInfoList){
        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.key).collect(Collectors.toList());
        return batchOperations.addChangeTypeOps(bucket, type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY,
                keyList.toArray(new String[]{}));
    }
}
