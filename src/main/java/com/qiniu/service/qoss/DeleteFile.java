package com.qiniu.service.qoss;

import com.qiniu.storage.BucketManager.BatchOperations;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DeleteFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    public DeleteFile(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath,
                      int saveIndex) throws IOException {
        super("delete", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
    }

    public DeleteFile(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, bucket, savePath, 0);
    }

    synchronized public BatchOperations getOperations(List<Map<String, String>> lineList) {
        batchOperations.clearOps();
        lineList.forEach(line -> {
            if (line.get("key") == null)
                errorLineList.add(String.valueOf(line) + "\tno target key in the line map.");
            else
                batchOperations.addDeleteOp(bucket, line.get("key"));
        });
        return batchOperations;
    }

    public String getInputParams(Map<String, String> line) {
        return line.get("key");
    }
}
