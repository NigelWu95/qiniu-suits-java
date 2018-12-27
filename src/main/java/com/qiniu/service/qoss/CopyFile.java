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
    private boolean keepKey;
    private String keyPrefix;

    public CopyFile(Auth auth, Configuration configuration, String bucket, String toBucket, boolean keepKey,
                    String keyPrefix, String resultPath, int resultIndex) throws IOException {
        super(auth, configuration, bucket, "copy", resultPath, resultIndex);
        this.toBucket = toBucket;
        this.keepKey = keepKey;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
    }

    public CopyFile(Auth auth, Configuration configuration, String bucket, String toBucket, boolean keepKey,
                    String keyPrefix, String resultPath) throws IOException {
        this(auth, configuration, bucket, toBucket, keepKey, keyPrefix, resultPath, 0);
    }

    protected String processLine(Map<String, String> line) throws QiniuException {
        Response response = bucketManager.copy(bucket, line.get("key"), toBucket, keepKey ? keyPrefix +
                line.get("key") : null, false);
        return response.statusCode + "\t" + HttpResponseUtils.getResult(response);
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> lineList) {

        List<String> keyList = lineList.stream().map(line -> line.get("key")).collect(Collectors.toList());
        if (keepKey) {
            keyList.forEach(fileKey -> batchOperations.addCopyOp(bucket, fileKey, toBucket, keyPrefix + fileKey));
        } else {
            keyList.forEach(fileKey -> batchOperations.addCopyOp(bucket, fileKey, toBucket, null));
        }

        return batchOperations;
    }
}
