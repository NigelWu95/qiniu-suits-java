package com.qiniu.service.qoss;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CopyFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private String toBucket;
    private boolean keepKey;
    private String keyPrefix;

    private void initBaseParams(String toBucket) {
        this.processName = "copy";
        this.toBucket = toBucket;
    }

    public CopyFile(Auth auth, Configuration configuration, String fromBucket, String toBucket, String resultFileDir,
                    int resultFileIndex) throws IOException {
        super(auth, configuration, fromBucket, resultFileDir);
        initBaseParams(toBucket);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public CopyFile(Auth auth, Configuration configuration, String fromBucket, String toBucket, String resultFileDir) {
        super(auth, configuration, fromBucket, resultFileDir);
        initBaseParams(toBucket);
    }

    public void setOptions(boolean keepKey, String keyPrefix) {
        this.keepKey = keepKey;
        this.keyPrefix = keyPrefix;
    }

    public CopyFile getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        CopyFile copyFile = (CopyFile)super.clone();
        copyFile.fileMap = new FileMap();
        try {
            copyFile.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return copyFile;
    }

    protected Response getResponse(Map<String, String> fileInfo) throws QiniuException {
        return bucketManager.copy(bucket, fileInfo.get("key"), toBucket, keepKey ? keyPrefix +
                fileInfo.get("key") : null, false);
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> fileInfoList) {

        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.get("key")).collect(Collectors.toList());
        if (keepKey) {
            keyList.forEach(fileKey -> batchOperations.addCopyOp(bucket, fileKey, toBucket, keyPrefix + fileKey));
        } else {
            keyList.forEach(fileKey -> batchOperations.addCopyOp(bucket, fileKey, toBucket, null));
        }

        return batchOperations;
    }
}
