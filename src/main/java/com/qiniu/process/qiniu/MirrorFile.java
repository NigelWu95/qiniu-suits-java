package com.qiniu.process.qiniu;

import com.qiniu.interfaces.IFileChecker;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.CloudApiUtils;
import com.qiniu.util.HttpRespUtils;

import java.io.IOException;
import java.util.Map;

public class MirrorFile extends Base<Map<String, String>> {

    private Configuration configuration;
    private BucketManager bucketManager;

    public MirrorFile(String accessKey, String secretKey, Configuration configuration, String bucket) throws IOException {
        super("mirror", accessKey, secretKey, bucket);
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
        CloudApiUtils.checkQiniu(bucketManager, bucket);
    }

    public MirrorFile(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath,
                      int saveIndex) throws IOException {
        super("mirror", accessKey, secretKey, bucket, savePath, saveIndex);
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
        CloudApiUtils.checkQiniu(bucketManager, bucket);
    }

    public MirrorFile(String accessKey, String secretKey, Configuration configuration, String bucket, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, bucket, savePath, 0);
    }

    @Override
    public MirrorFile clone() throws CloneNotSupportedException {
        MirrorFile mirrorFile = (MirrorFile) super.clone();
        mirrorFile.bucketManager = new BucketManager(Auth.create(accessId, secretKey), configuration);
        return mirrorFile;
    }

    @Override
    protected IFileChecker fileCheckerInstance() {
        return "stat".equals(checkType) ? CloudApiUtils.fileCheckerInstance(bucketManager, bucket) : key -> null;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("key is not exists or empty in " + line);
        if (iFileChecker.check(key) != null) throw new IOException("file exists");
        return HttpRespUtils.getResult(bucketManager.prefetch(bucket, key));
    }

    @Override
    public void closeResource() {
        super.closeResource();
        configuration = null;
        bucketManager = null;
    }
}
