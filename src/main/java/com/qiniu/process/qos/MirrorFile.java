package com.qiniu.process.qos;

import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.CloudAPIUtils;

import java.io.IOException;
import java.util.Map;

public class MirrorFile extends Base<Map<String, String>> {

    private Configuration configuration;
    private BucketManager bucketManager;

    public MirrorFile(String accessKey, String secretKey, Configuration configuration, String bucket) throws IOException {
        super("mirror", accessKey, secretKey, bucket);
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudAPIUtils.checkQiniu(bucketManager, bucket);
    }

    public MirrorFile(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath,
                      int saveIndex) throws IOException {
        super("mirror", accessKey, secretKey, bucket, savePath, saveIndex);
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudAPIUtils.checkQiniu(bucketManager, bucket);
    }

    public MirrorFile(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, bucket, savePath, 0);
    }

    public MirrorFile clone() throws CloneNotSupportedException {
        MirrorFile mirrorFile = (MirrorFile) super.clone();
        mirrorFile.bucketManager = new BucketManager(Auth.create(authKey1, authKey2), configuration.clone());
        return mirrorFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("no key in " + line);
        bucketManager.prefetch(bucket, key);
        return key;
    }

    @Override
    public void closeResource() {
        super.closeResource();
        configuration = null;
        bucketManager = null;
    }
}
