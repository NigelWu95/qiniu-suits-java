package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CopyFile extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    final private String toBucket;
    final private String newKeyIndex;
    final private String keyPrefix;
    final private String rmPrefix;

    public CopyFile(Auth auth, Configuration configuration, String bucket, String toBucket, String newKeyIndex,
                    String keyPrefix, String rmPrefix, String resultPath, int resultIndex) throws IOException {
        super("copy", auth, configuration, bucket, resultPath, resultIndex);
        this.toBucket = toBucket;
        // 没有传入的 newKeyIndex 参数的话直接设置为默认的 "key"
        this.newKeyIndex = newKeyIndex == null || "".equals(newKeyIndex) ? "key" : newKeyIndex;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
        this.rmPrefix = rmPrefix == null ? "" : rmPrefix;
    }

    public CopyFile(Auth auth, Configuration configuration, String bucket, String toBucket, String newKeyIndex,
                    String keyPrefix, String rmPrefix, String resultPath) throws IOException {
        this(auth, configuration, bucket, toBucket, newKeyIndex, keyPrefix, rmPrefix, resultPath, 0);
    }

    private String formatKey(String key) {
        return keyPrefix + key.substring(0, rmPrefix.length()).replace(rmPrefix, "")
                + key.substring(rmPrefix.length());
    }

    public String processLine(Map<String, String> line) throws QiniuException {
        if (StringUtils.isNullOrEmpty(line.get(newKeyIndex))) {
            errorLineList.add(String.valueOf(line) + "\tno target " + newKeyIndex + " in the line map.");
            throw new QiniuException(null, "\tno target " + newKeyIndex + " in the line map.");
        }
        Response response = bucketManager.copy(bucket, line.get("key"), toBucket, formatKey(line.get(newKeyIndex)),
                false);
        return "{\"code\":" + response.statusCode + ",\"message\":\"" + HttpResponseUtils.getResult(response) + "\"}";
    }

    synchronized public BatchOperations getOperations(List<Map<String, String>> lineList) {
        lineList.forEach(line -> {
            if (StringUtils.isNullOrEmpty(line.get("key")) || StringUtils.isNullOrEmpty(line.get(newKeyIndex)))
                errorLineList.add(String.valueOf(line) + "\tno target key in the line map.");
            else
                batchOperations.addCopyOp(bucket, line.get("key"), toBucket, formatKey(line.get(newKeyIndex)));
        });
        return batchOperations;
    }

    public String getInputParams(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(newKeyIndex);
    }
}
