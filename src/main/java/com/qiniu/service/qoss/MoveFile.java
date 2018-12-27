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

public class MoveFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private String toBucket;
    private String newKeyIndex;
    private boolean forceIfOnlyPrefix;
    private String keyPrefix;

    public MoveFile(Auth auth, Configuration configuration, String bucket, String toBucket, String newKeyIndex,
                    String keyPrefix, boolean forceIfOnlyPrefix, String resultPath, int resultIndex) throws IOException {
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
    }

    public MoveFile(Auth auth, Configuration configuration, String bucket, String toBucket, String newKeyIndex,
                    String keyPrefix, boolean forceIfOnlyPrefix, String resultPath) throws IOException {
        this(auth, configuration, bucket, toBucket, newKeyIndex, keyPrefix, forceIfOnlyPrefix, resultPath, 0);
    }

    protected String processLine(Map<String, String> line) throws QiniuException {
        Response response;
        if (newKeyIndex != null) {
            response = bucketManager.rename(bucket, line.get("key"), keyPrefix + line.get(newKeyIndex),
                    false);
        } else {
            if (forceIfOnlyPrefix)
                response = bucketManager.rename(bucket, line.get("key"), keyPrefix + line.get("key"),
                        false);
            else
                response = bucketManager.move(bucket, line.get("key"), toBucket, keyPrefix + line.get("key"),
                    false);
        }
        return response.statusCode + "\t" + HttpResponseUtils.getResult(response);
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> lineList) {
        if (newKeyIndex != null) {
            for (Map<String, String> line : lineList) {
                batchOperations.addRenameOp(bucket, line.get("key"), keyPrefix + line.get(newKeyIndex));
            }
        } else {
            if (forceIfOnlyPrefix) {
                for (Map<String, String> line : lineList) {
                    batchOperations.addRenameOp(bucket, line.get("key"), keyPrefix + line.get("key"));
                }
            } else {
                for (Map<String, String> line : lineList) {
                    batchOperations.addMoveOp(bucket, line.get("key"), toBucket, keyPrefix + line.get("key"));
                }
            }
        }

        return batchOperations;
    }
}
