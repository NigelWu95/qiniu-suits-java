package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DeleteFile extends Base {

    private BucketManager bucketManager;

    public DeleteFile(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                      String savePath, int saveIndex) throws IOException {
        super("delete", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
        this.batchSize = 1000;
    }

    public DeleteFile(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                      String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, rmPrefix, savePath, 0);
    }

    public DeleteFile clone() throws CloneNotSupportedException {
        DeleteFile deleteFile = (DeleteFile)super.clone();
        deleteFile.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        return deleteFile;
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws QiniuException {
        BatchOperations batchOperations = new BatchOperations();
        lineList.forEach(line -> batchOperations.addDeleteOp(bucket, line.get("key")));
        return HttpResponseUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        return HttpResponseUtils.getResult(bucketManager.delete(bucket, line.get("key")));
    }
}
