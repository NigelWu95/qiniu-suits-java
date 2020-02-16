package com.qiniu.process.qiniu;

import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.CloudApiUtils;

import java.io.IOException;
import java.util.Map;

public class DomainsOfBucket extends Base<Map<String, String>> {

    private Configuration configuration;
    private BucketManager bucketManager;

    public DomainsOfBucket(String accessKey, String secretKey, Configuration configuration) throws IOException {
        super("domainsofbucket", accessKey, secretKey, null);
        this.configuration = configuration;
        Auth auth = Auth.create(accessKey, secretKey);
        this.bucketManager = new BucketManager(auth, configuration.clone());
        CloudApiUtils.checkQiniu(auth);
    }

    public DomainsOfBucket(String accessKey, String secretKey, Configuration configuration, String savePath, int saveIndex) throws IOException {
        super("domainsofbucket", accessKey, secretKey, null, savePath, saveIndex);
        this.configuration = configuration;
        Auth auth = Auth.create(accessKey, secretKey);
        this.bucketManager = new BucketManager(auth, configuration.clone());
        CloudApiUtils.checkQiniu(auth);
    }

    public DomainsOfBucket(String accessKey, String secretKey, Configuration configuration, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, savePath, 0);
    }

    @Override
    public DomainsOfBucket clone() throws CloneNotSupportedException {
        DomainsOfBucket mirrorFile = (DomainsOfBucket) super.clone();
        mirrorFile.bucketManager = new BucketManager(Auth.create(accessId, secretKey), configuration.clone());
        return mirrorFile;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("bucket");
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String bucket = line.get("bucket");
        if (bucket == null) throw new IOException("bucket is not exists or empty in " + line);
        return String.join(",", bucketManager.domainList(bucket));
    }

    @Override
    public void closeResource() {
        super.closeResource();
        configuration = null;
        bucketManager = null;
    }
}
