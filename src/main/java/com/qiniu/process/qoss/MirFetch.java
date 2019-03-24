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

public class MirFetch extends Base {

    private BucketManager bucketManager;

    public MirFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                    String savePath, int saveIndex) throws IOException {
        super("mirror", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
    }

    public MirFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                    String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, rmPrefix, savePath, 0);
    }

    public MirFetch clone() throws CloneNotSupportedException {
        MirFetch mirrorFetch = (MirFetch) super.clone();
        mirrorFetch.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        return mirrorFetch;
    }

    protected Response batchResult(List<Map<String, String>> lineList) throws QiniuException {
        return null;
    }

    protected String singleResult(Map<String, String> line) throws QiniuException {
        bucketManager.prefetch(bucket, line.get("key"));
        return line.get("key") + "\t" + "200";
    }
}
