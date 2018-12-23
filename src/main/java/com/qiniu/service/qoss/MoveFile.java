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
    private String keyPrefix;

    public MoveFile(Auth auth, Configuration configuration, String bucket, String toBucket, String keyPrefix,
                    String resultPath, int resultIndex) throws IOException {
        super(auth, configuration, bucket, toBucket == null || "".equals(toBucket) ? "rename" : "move",
                resultPath, resultIndex);
        this.toBucket = toBucket;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
    }

    public MoveFile(Auth auth, Configuration configuration, String bucket, String toBucket, String keyPrefix,
                    String resultPath) throws IOException {
        this(auth, configuration, bucket, toBucket, keyPrefix, resultPath, 0);
    }

    protected String processLine(Map<String, String> line) throws QiniuException {
        Response response;
        if (toBucket == null || "".equals(toBucket)) {
            response = bucketManager.move(bucket, line.get("key"), toBucket, keyPrefix + line.get("key"),
                    false);
        } else {
            response = bucketManager.rename(bucket, line.get("key"), keyPrefix + line.get("newKey"),
                    false);
        }
        return response.statusCode + "\t" + HttpResponseUtils.getResult(response);
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> lineList) {

        if (toBucket == null || "".equals(toBucket)) {
            lineList.forEach(line -> batchOperations.addMoveOp(bucket, line.get("key"), toBucket,
                    keyPrefix + line.get("key")));
        } else {
            lineList.forEach(line -> batchOperations.addRenameOp(bucket, line.get("key"),
                            keyPrefix + line.get("newKey")));
        }

        return batchOperations;
    }
}
