package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MoveFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private String toBucket;
    private String newKeyIndex;
    private boolean forceIfOnlyPrefix;
    private String keyPrefix;
    private String rmPrefix;

    public MoveFile(Auth auth, Configuration configuration, String bucket, String toBucket, String newKeyIndex,
                    String keyPrefix, String rmPrefix, boolean forceIfOnlyPrefix, String resultPath, int resultIndex)
            throws IOException {
        super(auth, configuration, bucket, toBucket == null || "".equals(toBucket) ? "rename" : "move",
                resultPath, resultIndex);
        if (newKeyIndex == null || "".equals(newKeyIndex)) {
            this.newKeyIndex = null;
            if (toBucket == null || "".equals(toBucket)) {
                if (forceIfOnlyPrefix) {
                    if (keyPrefix == null || "".equals(keyPrefix))
                        throw new IOException("although prefix-force is true, but the add-prefix is empty.");
                    else this.forceIfOnlyPrefix = true;
                } else {
                    throw new IOException("there is no newKey index, if you only want to add prefix for renaming, " +
                            "please set the \"prefix-force\" as true.");
                }
            } else this.toBucket = toBucket;
        } else this.newKeyIndex = newKeyIndex;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
        this.rmPrefix = rmPrefix == null ? "" : rmPrefix;
    }

    public MoveFile(Auth auth, Configuration configuration, String bucket, String toBucket, String newKeyIndex,
                    String keyPrefix, String rmPrefix, boolean forceIfOnlyPrefix, String resultPath) throws IOException {
        this(auth, configuration, bucket, toBucket, newKeyIndex, keyPrefix, rmPrefix, forceIfOnlyPrefix,
                resultPath, 0);
    }

    private String formatKey(String key) {
        return keyPrefix + key.substring(0, rmPrefix.length()).replace(rmPrefix, "")
                + key.substring(rmPrefix.length());
    }

    protected String processLine(Map<String, String> line) throws QiniuException {
        Response response;
        if (newKeyIndex != null) {
            response = bucketManager.rename(bucket, line.get("key"), keyPrefix + line.get(newKeyIndex),
                    false);
        } else {
            if (forceIfOnlyPrefix)
                response = bucketManager.rename(bucket, line.get("key"), formatKey(line.get("key")), false);
            else
                response = bucketManager.move(bucket, line.get("key"), toBucket, formatKey(line.get("key")), false);
        }
        return response.statusCode + "\t" + HttpResponseUtils.getResult(response);
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> lineList) {
        if (newKeyIndex != null) {
            for (Map<String, String> line : lineList) {
                batchOperations.addRenameOp(bucket, line.get("key"), keyPrefix + line.get(newKeyIndex));
            }
        } else {
            List<String> keyList = lineList.stream().map(line -> line.get("key")).collect(Collectors.toList());
            keyList.forEach(fileKey -> batchOperations.addCopyOp(bucket, fileKey, toBucket, formatKey(fileKey)));
            if (forceIfOnlyPrefix) {
                keyList.forEach(fileKey -> batchOperations.addRenameOp(bucket, fileKey, formatKey(fileKey)));
            } else {
                keyList.forEach(fileKey -> batchOperations.addMoveOp(bucket, fileKey, toBucket, formatKey(fileKey)));
            }
        }

        return batchOperations;
    }
}
