package com.qiniu.service.qoss;

import com.qiniu.storage.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.FileNameUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CopyFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    final private String toBucket;
    final private String newKeyIndex;
    final private String keyPrefix;
    final private String rmPrefix;

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String newKeyIndex, String keyPrefix, String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("copy", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
        this.toBucket = toBucket;
        // 没有传入的 newKeyIndex 参数的话直接设置为默认的 "key"
        this.newKeyIndex = newKeyIndex == null || "".equals(newKeyIndex) ? "key" : newKeyIndex;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
        this.rmPrefix = rmPrefix == null ? "" : rmPrefix;
    }

    public CopyFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String newKeyIndex, String keyPrefix, String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, toBucket, newKeyIndex, keyPrefix, rmPrefix, savePath, 0);
    }

    synchronized public BatchOperations getOperations(List<Map<String, String>> lineList) {
        batchOperations.clearOps();
        lineList.forEach(line -> {
            if (line.get("key") == null || line.get(newKeyIndex) == null) {
                errorLineList.add(String.valueOf(line) + "\tno target key in the line map.");
            } else {
                try {
                    String newKey = keyPrefix + FileNameUtils.rmPrefix(rmPrefix, line.get(newKeyIndex));
                    batchOperations.addCopyOp(bucket, line.get("key"), toBucket, newKey);
                } catch (IOException e) {
                    errorLineList.add(String.valueOf(line) + "\t" + e.getMessage());
                }
            }
        });
        return batchOperations;
    }

    public String getInputParams(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(newKeyIndex);
    }
}
