package com.qiniu.datasource;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.auth.DefaultCredentials;
import com.aliyun.oss.model.OSSObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.Converter;
import com.qiniu.convert.JsonObjectPair;
import com.qiniu.convert.StringBuilderPair;
import com.qiniu.convert.StringMapPair;
import com.qiniu.interfaces.IStorageLister;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.interfaces.IResultOutput;
import com.qiniu.util.CloudApiUtils;
import com.qiniu.util.ConvertingUtils;
import com.qiniu.util.FileUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AliOssContainer extends CloudStorageContainer<OSSObjectSummary, Map<String, String>> {

//    private String accessKeyId;
//    private String accessKeySecret;
    private Credentials credentials;
    private ClientConfiguration clientConfig;
    private String endpoint;

    public AliOssContainer(String accessKeyId, String accessKeySecret, ClientConfiguration clientConfig, String endpoint,
                           String bucket, Map<String, Map<String, String>> prefixesMap, List<String> antiPrefixes,
                           boolean prefixLeft, boolean prefixRight, Map<String, String> indexMap, List<String> fields,
                           int unitLen, int threads) throws IOException {
        super(bucket, prefixesMap, antiPrefixes, prefixLeft, prefixRight, indexMap, fields, unitLen, threads);
        this.credentials = new DefaultCredentials(accessKeyId, accessKeySecret);
//        this.accessKeyId = accessKeyId;
//        this.accessKeySecret = accessKeySecret;
        this.clientConfig = clientConfig;
        this.endpoint = endpoint;
        AliLister aliLister = new AliLister(new OSSClient(endpoint, new DefaultCredentialProvider(credentials),
                clientConfig), bucket, null, null, null, 1);
        aliLister.close();
        aliLister = null;
        OSSObjectSummary test = new OSSObjectSummary();
        test.setKey("test");
        ConvertingUtils.toPair(test, indexMap, new StringMapPair());
    }

    @Override
    public String getSourceName() {
        return "aliyun";
    }

    @Override
    protected ITypeConvert<OSSObjectSummary, Map<String, String>> getNewConverter() {
        return new Converter<OSSObjectSummary, Map<String, String>>() {
            @Override
            public Map<String, String> convertToV(OSSObjectSummary line) throws IOException {
                return ConvertingUtils.toPair(line, indexMap, new StringMapPair());
            }
        };
    }

    @Override
    protected ITypeConvert<OSSObjectSummary, String> getNewStringConverter() {
        IStringFormat<OSSObjectSummary> stringFormatter;
        if ("json".equals(saveFormat)) {
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new JsonObjectPair()).toString();
        } else if ("yaml".equals(saveFormat)) {
            stringFormatter = line -> ConvertingUtils.toStringWithIndent(line, fields);
        } else {
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new StringBuilderPair(saveSeparator));
        }
        return new Converter<OSSObjectSummary, String>() {
            @Override
            public String convertToV(OSSObjectSummary line) throws IOException {
                return stringFormatter.toFormatString(line);
            }
        };
    }

    @Override
    protected IResultOutput getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected IStorageLister<OSSObjectSummary> getLister(String prefix, String marker, String start, String end, int unitLen) throws SuitsException {
        if (marker == null || "".equals(marker)) marker = CloudApiUtils.getAliOssMarker(start);
        try {
            return new AliLister(new OSSClient(endpoint, new DefaultCredentialProvider(credentials),
                    clientConfig), bucket, prefix, marker, end, unitLen);
        } catch (SuitsException e) {
            if (e.getMessage().endsWith("Invalid byte 1 of 1-byte UTF-8 sequence.")) {
                try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                errorLogger.error("generate lister by prefix:{} retrying...", prefix, e);
                return new AliLister(new OSSClient(endpoint, new DefaultCredentialProvider(credentials),
                        clientConfig), bucket, prefix, marker, end, unitLen, "url");
            } else {
                throw e;
            }
        }
    }
}
