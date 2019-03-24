package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.StorageType;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ChgType extends Base {

    final private int type;
    private BucketManager bucketManager;

    public ChgType(String accessKey, String secretKey, Configuration configuration, String bucket, int type,
                   String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("type", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.type = type;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public ChgType(String accessKey, String secretKey, Configuration configuration, String bucket, int type,
                   String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, type, rmPrefix, savePath, 0);
    }

    public ChgType clone() throws CloneNotSupportedException {
        ChgType changeType = (ChgType)super.clone();
        changeType.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        return changeType;
    }

    protected Response batchResult(List<Map<String, String>> lineList) throws QiniuException {
        BatchOperations batchOperations = new BatchOperations();
        lineList.forEach(line -> batchOperations.addChangeTypeOps(bucket, type == 0 ? StorageType.COMMON :
                StorageType.INFREQUENCY, line.get("key")));
        return bucketManager.batch(batchOperations);
    }

    protected String singleResult(Map<String, String> line) throws QiniuException {
        return null;
    }
}
