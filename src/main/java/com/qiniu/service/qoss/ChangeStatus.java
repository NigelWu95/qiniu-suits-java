package com.qiniu.service.qoss;

import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ChangeStatus extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    final private int status;

    public ChangeStatus(String accessKey, String secretKey, Configuration configuration, String bucket, int status,
                        String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("status", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.status = status;
    }

    public ChangeStatus(String accessKey, String secretKey, Configuration configuration, String bucket, int status,
                        String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, status, rmPrefix, savePath, 0);
    }

    synchronized public BucketManager.BatchOperations getBatchOperations(List<Map<String, String>> lineList) {
        batchOperations.clearOps();
        lineList.forEach(line -> batchOperations.addChangeStatusOps(bucket, status, line.get("key")));
        return batchOperations;
    }

    public String getInputParams(Map<String, String> line) {
        return line.get("key");
    }
}
