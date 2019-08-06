package com.qiniu.datasource;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.Converter;
import com.qiniu.convert.JsonObjectPair;
import com.qiniu.convert.StringMapPair;
import com.qiniu.interfaces.ILister;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.interfaces.IResultOutput;
import com.qiniu.util.ConvertingUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AwsS3Container extends CloudStorageContainer<S3ObjectSummary, BufferedWriter, Map<String, String>> {

    private String accessKeyId;
    private String secretKey;
    private ClientConfiguration clientConfig;
    private String region;
    private Map<String, String> indexPair;
    private List<String> fields;

    public AwsS3Container(String accessKeyId, String secretKey, ClientConfiguration clientConfig, String region,
                          String bucket, List<String> antiPrefixes, Map<String, Map<String, String>> prefixesMap,
                          boolean prefixLeft, boolean prefixRight, Map<String, String> indexMap, List<String> fields,
                          int unitLen, int threads) throws SuitsException {
        super(bucket, antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        this.accessKeyId = accessKeyId;
        this.secretKey = secretKey;
        this.clientConfig = clientConfig;
        this.region = region;
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretKey)))
                .withRegion(region)
                .withClientConfiguration(clientConfig)
                .build();
        AwsS3Lister awsS3Lister = new AwsS3Lister(s3Client, bucket, null, null, null, null, 1);
        awsS3Lister.close();
        awsS3Lister = null;
        indexPair = ConvertingUtils.getReversedIndexMap(indexMap, rmFields);
        if (fields == null || fields.size() == 0) this.fields = ConvertingUtils.getKeyOrderFields(indexPair);
        else this.fields = fields;
    }

    @Override
    public String getSourceName() {
        return "s3";
    }

    @Override
    protected ITypeConvert<S3ObjectSummary, Map<String, String>> getNewConverter() {
        return new Converter<S3ObjectSummary, Map<String, String>>() {
            @Override
            public Map<String, String> convertToV(S3ObjectSummary line) throws IOException {
                return ConvertingUtils.toPair(line, indexMap, new StringMapPair());
            }
        };
    }

    @Override
    protected ITypeConvert<S3ObjectSummary, String> getNewStringConverter() {
        IStringFormat<S3ObjectSummary> stringFormatter;
        if ("json".equals(saveFormat)) {
            stringFormatter = line -> ConvertingUtils.toPair(line, indexPair, new JsonObjectPair()).toString();
        } else {
            stringFormatter = line -> ConvertingUtils.toFormatString(line, saveSeparator, fields);
        }
        return new Converter<S3ObjectSummary, String>() {
            @Override
            public String convertToV(S3ObjectSummary line) throws IOException {
                return stringFormatter.toFormatString(line);
            }
        };
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected ILister<S3ObjectSummary> getLister(String prefix, String marker, String start, String end) throws SuitsException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretKey)))
                .withRegion(region)
                .withClientConfiguration(clientConfig)
                .build();
        return new AwsS3Lister(s3Client, bucket, prefix, marker, start, end, unitLen);
    }
}
