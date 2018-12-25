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
    private String keyPrefix;
    private boolean forceIfOnlyPrefix;

    public MoveFile(Auth auth, Configuration configuration, String bucket, String toBucket, String newKeyIndex,
                    String keyPrefix, boolean forceIfOnlyPrefix, String resultPath, int resultIndex) throws IOException {
        super(auth, configuration, bucket, toBucket == null || "".equals(toBucket) ? "rename" : "move",
                resultPath, resultIndex);
        this.toBucket = toBucket;
        if (newKeyIndex == null || "".equals(newKeyIndex)) {
            if (forceIfOnlyPrefix) {
                if (keyPrefix == null || "".equals(keyPrefix)) throw new IOException("the add-prefix is empty.");
            } else {
                throw new IOException("there is no newKey index, if you only want to add prefix for renaming, " +
                        "please set the \"\"prefix-force\"\" as true.");
            }
        } else {
            this.newKeyIndex = newKeyIndex;
        }
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
        this.forceIfOnlyPrefix = forceIfOnlyPrefix;
    }

    public MoveFile(Auth auth, Configuration configuration, String bucket, String toBucket, String newKeyIndex,
                    String keyPrefix, boolean forceIfOnlyPrefix, String resultPath) throws IOException {
        this(auth, configuration, bucket, toBucket, newKeyIndex, keyPrefix, forceIfOnlyPrefix, resultPath, 0);
    }

    protected String processLine(Map<String, String> line) throws QiniuException {
        Response response;
        if (toBucket == null || "".equals(toBucket)) {
            response = bucketManager.rename(bucket, line.get("key"), keyPrefix + line.get(newKeyIndex),
                    false);
        } else {
            response = bucketManager.move(bucket, line.get("key"), toBucket, keyPrefix + line.get("key"),
                    false);
        }
        return response.statusCode + "\t" + HttpResponseUtils.getResult(response);
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> lineList) throws QiniuException {
        List<String> keyList = lineList.stream().map(line -> line.get("key"))
                .filter(key -> key != null && !"".equals(key)).collect(Collectors.toList());
        if (keyList.size() == 0) throw new QiniuException(null, "there is no key in line.");
        if (toBucket == null || "".equals(toBucket)) {
            List<String> newKeyList = lineList.stream().map(line -> line.get(newKeyIndex))
                    .filter(key -> key != null && !"".equals(key)).collect(Collectors.toList());
            if (newKeyList.size() == 0 && forceIfOnlyPrefix) {
                for (String key : keyList) {
                    batchOperations.addRenameOp(bucket, key, keyPrefix + key);
                }
            }
            if (keyList.size() != newKeyList.size())
                throw new QiniuException(null, "there are no corresponding keys in line.");
            for (int i = 0; i < keyList.size(); i++) {
                batchOperations.addRenameOp(bucket, keyList.get(i), keyPrefix + newKeyList.get(i));
            }
        } else {
            for (String key : keyList) {
                batchOperations.addMoveOp(bucket, key, toBucket, keyPrefix + key);
            }
        }

        return batchOperations;
    }
}
