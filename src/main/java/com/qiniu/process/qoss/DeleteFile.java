package com.qiniu.process.qoss;

import com.qiniu.interfaces.ILineProcess;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DeleteFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    public DeleteFile(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                      String savePath, int saveIndex) throws IOException {
        super("delete", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
    }

    public DeleteFile(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                      String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, rmPrefix, savePath, 0);
    }

    synchronized public BucketManager.BatchOperations getBatchOperations(List<Map<String, String>> lineList) {
        batchOperations.clearOps();
        lineList.forEach(line -> batchOperations.addDeleteOp(bucket, line.get("key")));
        return batchOperations;
    }

    public String getInputParams(Map<String, String> line) {
        return line.get("key");
    }
}
