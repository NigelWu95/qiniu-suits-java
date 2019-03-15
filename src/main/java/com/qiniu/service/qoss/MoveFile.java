package com.qiniu.service.qoss;

import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.FileNameUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MoveFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    final private String toBucket;
    final private String newKeyIndex;
    final private String keyPrefix;
    final private String rmPrefix;

    public MoveFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String newKeyIndex, String keyPrefix, String rmPrefix, boolean forceIfOnlyPrefix, String savePath,
                    int saveIndex) throws IOException {
        // 目标 bucket 为空时规定为 rename 操作
        super(toBucket == null || "".equals(toBucket) ? "rename" : "move", accessKey, secretKey, configuration, bucket,
                savePath, saveIndex);
        if (newKeyIndex == null || "".equals(newKeyIndex)) {
            this.newKeyIndex = "key";
            if (toBucket == null || "".equals(toBucket)) {
                // rename 操作时未设置 new-key 的条件判断
                if (forceIfOnlyPrefix) {
                    if (keyPrefix == null || "".equals(keyPrefix))
                        throw new IOException("although prefix-force is true, but the add-prefix is empty.");
                } else {
                    throw new IOException("there is no newKey index, if you only want to add prefix for renaming, " +
                            "please set the \"prefix-force\" as true.");
                }
            }
        } else {
            this.newKeyIndex = newKeyIndex;
        }
        this.toBucket = toBucket;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
        this.rmPrefix = rmPrefix;
    }

    public MoveFile(String accessKey, String secretKey, Configuration configuration, String bucket, String toBucket,
                    String newKeyIndex, String keyPrefix, String rmPrefix, boolean forceIfOnlyPrefix, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, bucket, toBucket, newKeyIndex, keyPrefix, rmPrefix, forceIfOnlyPrefix,
                savePath, 0);
    }

    synchronized public List<Map<String, String>> setBatchOperations(List<Map<String, String>> lineList) {
        batchOperations.clearOps();
        return lineList.parallelStream().filter(line -> {
            try {
                if (toBucket == null || "".equals(toBucket)) {
                    batchOperations.addRenameOp(bucket, line.get("key"), keyPrefix +
                            FileNameUtils.rmPrefix(rmPrefix, line.get(newKeyIndex)));
                } else {
                    batchOperations.addMoveOp(bucket, line.get("key"), toBucket, keyPrefix +
                            FileNameUtils.rmPrefix(rmPrefix, line.get(newKeyIndex)));
                }
                return true;
            } catch (IOException e) {
                errorLineList.add(String.valueOf(line) + "\t" + e.getMessage());
                return false;
            }
        }).collect(Collectors.toList());
    }

    public String getInputParams(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(newKeyIndex);
    }
}
