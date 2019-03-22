package com.qiniu.process.qoss;

import com.qiniu.interfaces.ILineProcess;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.StorageType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ChangeType extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    final private int type;

    public ChangeType(String accessKey, String secretKey, Configuration configuration, String bucket, int type,
                      String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("type", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.type = type;
    }

    public ChangeType(String accessKey, String secretKey, Configuration configuration, String bucket, int type,
                      String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, type, rmPrefix, savePath, 0);
    }

    synchronized public BucketManager.BatchOperations getBatchOperations(List<Map<String, String>> lineList) {
        batchOperations.clearOps();
        lineList.forEach(line -> batchOperations
                .addChangeTypeOps(bucket, type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY, line.get("key")));
        return batchOperations;
    }

    public String getInputParams(Map<String, String> line) {
        return line.get("key");
    }
}
