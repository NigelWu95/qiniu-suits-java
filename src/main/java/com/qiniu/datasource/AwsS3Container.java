package com.qiniu.datasource;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.Converter;
import com.qiniu.convert.JsonObjectPair;
import com.qiniu.convert.StringBuilderPair;
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

//    private String accessKeyId;
//    private String secretKey;
//    private ClientConfiguration clientConfig;
//    private String endpoint;
//    private String region;
    private AmazonS3ClientBuilder amazonS3ClientBuilder;

    public AwsS3Container(String accessKeyId, String secretKey, ClientConfiguration clientConfig, String endpoint,
                          String region, String bucket, Map<String, Map<String, String>> prefixesMap, List<String> antiPrefixes,
                          boolean prefixLeft, boolean prefixRight, Map<String, String> indexMap, List<String> fields,
                          int unitLen, int threads) throws IOException {
        super(bucket, prefixesMap, antiPrefixes, prefixLeft, prefixRight, indexMap, fields, unitLen, threads);
//        this.accessKeyId = accessKeyId;
//        this.secretKey = secretKey;
//        this.clientConfig = clientConfig;
//        this.endpoint = endpoint;
//        this.region = region;
        if (endpoint == null || endpoint.isEmpty()) {
            amazonS3ClientBuilder = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretKey)))
                    .withClientConfiguration(clientConfig)
                    .withRegion(region);
        } else {
            EndpointConfiguration endpointConfiguration = new EndpointConfiguration(endpoint, region);
            amazonS3ClientBuilder = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretKey)))
                    .withClientConfiguration(clientConfig)
                    .withEndpointConfiguration(endpointConfiguration);
        }
//        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretKey)))
//                .withRegion(region)
//                .withClientConfiguration(clientConfig)
//                .build();
        AmazonS3 s3Client = amazonS3ClientBuilder.build();
        AwsS3Lister awsS3Lister = new AwsS3Lister(s3Client, bucket, null, null, null, null, 1);
        awsS3Lister.close();
        awsS3Lister = null;
        S3ObjectSummary test = new S3ObjectSummary();
        test.setKey("test");
        ConvertingUtils.toPair(test, indexMap, new StringMapPair());
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
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new JsonObjectPair()).toString();
        } else {
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new StringBuilderPair(saveSeparator));
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
    protected ILister<S3ObjectSummary> getLister(String prefix, String marker, String start, String end, int unitLen) throws SuitsException {
//        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretKey)))
//                .withRegion(region)
//                .withClientConfiguration(clientConfig)
//                .build();
        return new AwsS3Lister(amazonS3ClientBuilder.build(), bucket, prefix, marker, start, end, unitLen);
    }
}
