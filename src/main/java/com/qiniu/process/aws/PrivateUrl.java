package com.qiniu.process.aws;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.Base;
import com.qiniu.util.CloudAPIUtils;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.Map;

public class PrivateUrl extends Base<Map<String, String>> {

    private String region;
    private Date expiration;
    private AmazonS3 s3Client;
    private ILineProcess<Map<String, String>> nextProcessor;

    public PrivateUrl(String accessKeyId, String secretKey, String bucket, String region, long expires,
                      String savePath, int saveIndex) throws IOException {
        super("awsprivate", accessKeyId, secretKey, bucket, savePath, saveIndex);
        this.region = region;
        expiration = new Date(System.currentTimeMillis() + expires);
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretKey)))
                .withRegion(region)
                .build();
        CloudAPIUtils.checkAws(s3Client);
    }

    public PrivateUrl(String accessKeyId, String secretKey, String bucket, String region, long expires) {
        super("awsprivate", accessKeyId, secretKey, bucket);
        this.region = region;
        expiration = new Date(System.currentTimeMillis() + expires);
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretKey)))
                .withRegion(region)
                .build();
        CloudAPIUtils.checkAws(s3Client);
    }

    public PrivateUrl(String accessKeyId, String accessKeySecret, String bucket, String endpoint, long expires,
                      String savePath) throws IOException {
        this(accessKeyId, accessKeySecret, bucket, endpoint, expires, savePath, 0);
    }

    public void updateEndpoint(String region) {
        this.region = region;
    }

    public void updateExpires(long expires) {
        this.expiration = new Date(System.currentTimeMillis() + expires);
    }

    public PrivateUrl clone() throws CloneNotSupportedException {
        PrivateUrl awsPrivateUrl = (PrivateUrl)super.clone();
        awsPrivateUrl.s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(authKey1, authKey2)))
                .withRegion(region)
                .build();
        if (nextProcessor != null) awsPrivateUrl.nextProcessor = nextProcessor.clone();
        return awsPrivateUrl;
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextProcessor = nextProcessor;
    }

    @Override
    public String singleResult(Map<String, String> line) throws Exception {
        String key = line.get("key");
        if (key == null) throw new IOException("no key in " + line);
        // 生成以GET方法访问的签名URL，访客可以直接通过浏览器访问相关内容。
        URL url = s3Client.generatePresignedUrl(bucket, key, expiration);
        if (nextProcessor != null) {
            line.put("url", url.toString());
            return nextProcessor.processLine(line);
        }
        return url.toString();
    }

    @Override
    public void closeResource() {
        super.closeResource();
        region = null;
        expiration = null;
        s3Client = null;
        nextProcessor = null;
    }
}
