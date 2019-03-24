package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DelFile extends Base {

    private BucketManager bucketManager;

    public DelFile(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                   String savePath, int saveIndex) throws IOException {
        super("delete", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
    }

    public DelFile(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                   String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, rmPrefix, savePath, 0);
    }

    public DelFile clone() throws CloneNotSupportedException {
        DelFile deleteFile = (DelFile)super.clone();
        deleteFile.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        return deleteFile;
    }

    protected Map<String, String> formatLine(Map<String, String> line) {
        return line;
    }

    protected String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    protected Response batchResult(List<Map<String, String>> lineList) throws QiniuException {
        BucketManager.BatchOperations batchOperations = new BucketManager.BatchOperations();
        lineList.forEach(line -> batchOperations.addDeleteOp(bucket, line.get("key")));
        return bucketManager.batch(batchOperations);
    }

    protected String singleResult(Map<String, String> line) throws QiniuException {
        return null;
    }
}
