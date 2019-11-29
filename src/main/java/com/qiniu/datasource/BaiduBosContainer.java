package com.qiniu.datasource;

import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.Converter;
import com.qiniu.convert.JsonObjectPair;
import com.qiniu.convert.StringBuilderPair;
import com.qiniu.convert.StringMapPair;
import com.qiniu.interfaces.IStorageLister;
import com.qiniu.interfaces.IResultOutput;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.util.CloudApiUtils;
import com.qiniu.util.ConvertingUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BaiduBosContainer extends CloudStorageContainer<BosObjectSummary, BufferedWriter, Map<String, String>> {

//    private String accessKeyId;
//    private String accessKeySecret;
    private BosClientConfiguration configuration;

    public BaiduBosContainer(String accessKeyId, String accessKeySecret, BosClientConfiguration configuration, String endpoint,
                             String bucket, Map<String, Map<String, String>> prefixesMap, List<String> antiPrefixes,
                             boolean prefixLeft, boolean prefixRight, Map<String, String> indexMap, List<String> fields,
                             int unitLen, int threads) throws IOException {
        super(bucket, prefixesMap, antiPrefixes, prefixLeft, prefixRight, indexMap, fields, unitLen, threads);
//        this.accessKeyId = accessKeyId;
//        this.accessKeySecret = accessKeySecret;
        this.configuration = configuration;
        this.configuration.setCredentials(new DefaultBceCredentials(accessKeyId, accessKeySecret));
        this.configuration.setEndpoint(endpoint);
        BosClient bosClient = new BosClient(this.configuration);
        try {
            BaiduLister baiduLister = new BaiduLister(bosClient, bucket, null, null, null, 1);
            baiduLister.close();
            baiduLister = null;
        } catch (SuitsException e) {
            bosClient.shutdown();
            throw e;
        }
        BosObjectSummary test = new BosObjectSummary();
        test.setKey("test");
        ConvertingUtils.toPair(test, indexMap, new StringMapPair());
    }

    @Override
    public String getSourceName() {
        return "baidu";
    }

    @Override
    protected ITypeConvert<BosObjectSummary, Map<String, String>> getNewConverter() {
        return new Converter<BosObjectSummary, Map<String, String>>() {
            @Override
            public Map<String, String> convertToV(BosObjectSummary line) throws IOException {
                return ConvertingUtils.toPair(line, indexMap, new StringMapPair());
            }
        };
    }

    @Override
    protected ITypeConvert<BosObjectSummary, String> getNewStringConverter() {
        IStringFormat<BosObjectSummary> stringFormatter;
        if ("json".equals(saveFormat)) {
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new JsonObjectPair()).toString();
        } else {
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new StringBuilderPair(saveSeparator));
        }
        return new Converter<BosObjectSummary, String>() {
            @Override
            public String convertToV(BosObjectSummary line) throws IOException {
                return stringFormatter.toFormatString(line);
            }
        };
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected IStorageLister<BosObjectSummary> getLister(String prefix, String marker, String start, String end, int unitLen) throws SuitsException {
        if (marker == null || "".equals(marker)) marker = CloudApiUtils.getAliOssMarker(start);
        BosClient bosClient = new BosClient(this.configuration);
        try {
            return new BaiduLister(new BosClient(configuration), bucket, prefix, marker, end, unitLen);
        } catch (SuitsException e) {
            bosClient.shutdown();
            throw e;
        }
    }
}
