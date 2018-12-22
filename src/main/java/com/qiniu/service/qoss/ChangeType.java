package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.StorageType;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ChangeType extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private int type;

    public ChangeType(Auth auth, Configuration configuration, String bucket, int type, String resultPath,
                      int resultIndex) throws IOException {
        super(auth, configuration, bucket, "type", resultPath, resultIndex);
        this.type = type;
    }

    public ChangeType(Auth auth, Configuration configuration, String bucket, int type, String resultPath)
            throws IOException {
        this(auth, configuration, bucket, type, resultPath, 0);
    }

    public ChangeType getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        ChangeType changeType = (ChangeType)super.clone();
        try {
            changeType.fileMap.initWriter(resultPath, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return changeType;
    }

    protected Response getResponse(Map<String, String> fileInfo) throws QiniuException {
        StorageType storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;
        return bucketManager.changeType(bucket, fileInfo.get("key"), storageType);
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> fileInfoList){
        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.get("key")).collect(Collectors.toList());
        return batchOperations.addChangeTypeOps(bucket, type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY,
                keyList.toArray(new String[]{}));
    }
}
