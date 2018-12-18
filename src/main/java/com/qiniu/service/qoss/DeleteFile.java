package com.qiniu.service.qoss;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.BatchOperations;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeleteFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private void initBaseParams() {
        this.processName = "delete";
    }

    public DeleteFile(Auth auth, Configuration configuration, String fromBucket, String resultFileDir,
                      int resultFileIndex) throws IOException {
        super(auth, configuration, fromBucket, resultFileDir);
        initBaseParams();
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public DeleteFile(Auth auth, Configuration configuration, String fromBucket, String resultFileDir) {
        super(auth, configuration, fromBucket, resultFileDir);
        initBaseParams();
    }

    public DeleteFile getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        DeleteFile copyFile = (DeleteFile)super.clone();
        copyFile.fileMap = new FileMap();
        try {
            copyFile.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return copyFile;
    }

    protected Response getResponse(Map<String, String> fileInfo) throws QiniuException {
        return bucketManager.delete(bucket, fileInfo.get("key"));
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> fileInfoList) {

        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.get("key")).collect(Collectors.toList());
        batchOperations.addDeleteOp(bucket, keyList.toArray(new String[]{}));
        return batchOperations;
    }
}
