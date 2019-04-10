package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.Map;

public class MirrorFile extends Base {

    private BucketManager bucketManager;

    public MirrorFile(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath,
                      int saveIndex) throws IOException {
        super("mirror", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
    }

    public void updateMirror(String bucket) {
        this.bucket = bucket;
    }

    public MirrorFile(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, bucket, savePath, 0);
    }

    public MirrorFile clone() throws CloneNotSupportedException {
        MirrorFile mirrorFile = (MirrorFile) super.clone();
        mirrorFile.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        return mirrorFile;
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        bucketManager.prefetch(bucket, line.get("key"));
        return line.get("key") + "\t" + "200";
    }
}
