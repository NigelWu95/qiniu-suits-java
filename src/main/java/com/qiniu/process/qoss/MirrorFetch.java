package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.FileNameUtils;

import java.io.IOException;
import java.util.Map;

public class MirrorFetch extends Base {

    private BucketManager bucketManager;

    public MirrorFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                       String savePath, int saveIndex) throws IOException {
        super("mirror", accessKey, secretKey, configuration, bucket, rmPrefix, savePath, saveIndex);
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
    }

    public MirrorFetch(String accessKey, String secretKey, Configuration configuration, String bucket, String rmPrefix,
                       String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, rmPrefix, savePath, 0);
    }

    public MirrorFetch clone() throws CloneNotSupportedException {
        MirrorFetch mirrorFetch = (MirrorFetch) super.clone();
        mirrorFetch.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        return mirrorFetch;
    }

    @Override
    protected Map<String, String> formatLine(Map<String, String> line) throws IOException {
        line.put("key", FileNameUtils.rmPrefix(rmPrefix, line.get("key")).replaceAll("\\?", "%3F"));
        return line;
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        bucketManager.prefetch(bucket, line.get("key"));
        return line.get("key") + "\t" + "200";
    }
}
