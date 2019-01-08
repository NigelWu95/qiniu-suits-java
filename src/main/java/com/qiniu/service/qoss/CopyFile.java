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

public class CopyFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    private String toBucket;
    private String newKeyIndex;
    private String keyPrefix;
    private String rmPrefix;

    public CopyFile(Auth auth, Configuration configuration, String bucket, String toBucket, String newKeyIndex,
                    String keyPrefix, String rmPrefix, String resultPath, int resultIndex) throws IOException {
        super(auth, configuration, bucket, "copy", resultPath, resultIndex);
        this.toBucket = toBucket;
        this.newKeyIndex = "".equals(newKeyIndex) ? null : newKeyIndex;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
        this.rmPrefix = rmPrefix == null ? "" : rmPrefix;
    }

    public CopyFile(Auth auth, Configuration configuration, String bucket, String toBucket, String newKeyIndex,
                    String keyPrefix, String rmPrefix, String resultPath) throws IOException {
        this(auth, configuration, bucket, toBucket, newKeyIndex, keyPrefix, rmPrefix, resultPath, 0);
    }

    private String formatKey(Map<String, String> line) {
        String tempKey = line.get(newKeyIndex);
        if (tempKey == null || "".equals(tempKey)) {
            return keyPrefix + line.get("key").substring(0, rmPrefix.length())
                    .replace(rmPrefix, "") + line.get("key").substring(rmPrefix.length());
        } else {
            return keyPrefix + tempKey.substring(0, rmPrefix.length()).replace(rmPrefix, "")
                    + tempKey.substring(rmPrefix.length());
        }
    }

    protected String processLine(Map<String, String> line) throws QiniuException {
        Response response = bucketManager.copy(bucket, line.get("key"), toBucket, formatKey(line), false);
        return response.statusCode + "\t" + HttpResponseUtils.getResult(response);
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> lineList) {
        lineList.forEach(line -> batchOperations.addCopyOp(bucket, line.get("key"), toBucket, formatKey(line)));
        return batchOperations;
    }
}
