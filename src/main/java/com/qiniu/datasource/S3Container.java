package com.qiniu.datasource;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.S3ObjToMap;
import com.qiniu.convert.S3ObjToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultOutput;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class S3Container extends CloudStorageContainer<S3ObjectSummary, BufferedWriter, Map<String, String>> {

    private String accessKeyId;
    private String secretKey;
    private ClientConfiguration clientConfig;
    private String region;

    public S3Container(String accessKeyId, String secretKey, ClientConfiguration clientConfig, String region,
                       String bucket, List<String> antiPrefixes, Map<String, String[]> prefixesMap, boolean prefixLeft,
                       boolean prefixRight, Map<String, String> indexMap, int unitLen, int threads) {
        super(bucket, antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        this.accessKeyId = accessKeyId;
        this.secretKey = secretKey;
        this.clientConfig = clientConfig;
        this.region = region;
    }

    @Override
    public String getSourceName() {
        return "s3";
    }

    @Override
    protected ITypeConvert<S3ObjectSummary, Map<String, String>> getNewConverter() {
        return new S3ObjToMap(indexMap);
    }

    @Override
    protected ITypeConvert<S3ObjectSummary, String> getNewStringConverter() throws IOException {
        return new S3ObjToString(saveFormat, saveSeparator, rmFields);
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected ILister<S3ObjectSummary> getLister(String prefix, String marker, String end) throws SuitsException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretKey)))
                .withRegion(region)
                .withClientConfiguration(clientConfig)
                .build();
        return new S3Lister(s3Client, bucket, prefix, marker, end, null, unitLen);
    }
}
