package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.persistence.FileMap;
import com.qiniu.sdk.BucketManager.BatchOperations;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MoveFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private String toBucket;
    private String keyPrefix;

    private void initBaseParams(String toBucket) {
        this.processName = toBucket == null || "".equals(toBucket) ? "rename" : "move";
        this.toBucket = toBucket;
    }

    public MoveFile(Auth auth, Configuration configuration, String fromBucket, String toBucket, String resultFileDir,
                    int resultFileIndex) throws IOException {
        super(auth, configuration, fromBucket, resultFileDir);
        initBaseParams(toBucket);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public MoveFile(Auth auth, Configuration configuration, String fromBucket, String toBucket, String resultFileDir) {
        super(auth, configuration, fromBucket, resultFileDir);
        initBaseParams(toBucket);
    }

    public void setOptions(String keyPrefix) {
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
    }

    public MoveFile getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        MoveFile copyFile = (MoveFile)super.clone();
        copyFile.fileMap = new FileMap();
        try {
            copyFile.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return copyFile;
    }

    protected Response getResponse(Map<String, String> fileInfo) throws QiniuException {
        if (toBucket == null || "".equals(toBucket)) {
            return bucketManager.move(bucket, fileInfo.get("key"), toBucket, keyPrefix + fileInfo.get("key"),
                    false);
        } else {
            return bucketManager.rename(bucket, fileInfo.get("key"), keyPrefix + fileInfo.get("newKey"),
                    false);
        }
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> fileInfoList) {

        if (toBucket == null || "".equals(toBucket)) {
            fileInfoList.forEach(fileInfo -> batchOperations.addMoveOp(bucket, fileInfo.get("key"), toBucket,
                    keyPrefix + fileInfo.get("key")));
        } else {
            fileInfoList.forEach(fileInfo -> batchOperations.addRenameOp(bucket, fileInfo.get("key"),
                            keyPrefix + fileInfo.get("newKey")));
        }

        return batchOperations;
    }
}
