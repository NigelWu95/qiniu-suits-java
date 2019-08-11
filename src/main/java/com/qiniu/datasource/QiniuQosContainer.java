package com.qiniu.datasource;

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
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.CloudAPIUtils;
import com.qiniu.util.ConvertingUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

public class QiniuQosContainer extends CloudStorageContainer<FileInfo, BufferedWriter, Map<String, String>> {

    private String accessKey;
    private String secretKey;
    private Configuration configuration;

    public QiniuQosContainer(String accessKey, String secretKey, Configuration configuration, String bucket,
                             Map<String, Map<String, String>> prefixesMap, List<String> antiPrefixes, boolean prefixLeft,
                             boolean prefixRight, Map<String, String> indexMap, List<String> fields, int unitLen,
                             int threads) throws IOException {
        super(bucket, prefixesMap, antiPrefixes, prefixLeft, prefixRight, indexMap, fields, unitLen, threads);
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.configuration = configuration;
        QiniuLister qiniuLister = new QiniuLister(new BucketManager(Auth.create(accessKey, secretKey), configuration),
                bucket, null, null, null, 1);
        qiniuLister.close();
        qiniuLister = null;
    }

    @Override
    public String getSourceName() {
        return "qiniu";
    }

    @Override
    protected ITypeConvert<FileInfo, Map<String, String>> getNewConverter() {
        return new Converter<FileInfo, Map<String, String>>() {
            @Override
            public Map<String, String> convertToV(FileInfo line) throws IOException {
                return ConvertingUtils.toPair(line, indexMap, new StringMapPair());
            }
        };
    }

    @Override
    protected ITypeConvert<FileInfo, String> getNewStringConverter() {
        IStringFormat<FileInfo> stringFormatter;
        if ("json".equals(saveFormat)) {
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new JsonObjectPair()).toString();
        } else {
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new StringBuilderPair(saveSeparator));
        }
        return new Converter<FileInfo, String>() {
            @Override
            public String convertToV(FileInfo line) throws IOException {
                return stringFormatter.toFormatString(line);
            }
        };
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected ILister<FileInfo> getLister(String prefix, String marker, String start, String end) throws SuitsException {
        if (marker == null || "".equals(marker)) marker = CloudAPIUtils.getQiniuMarker(start);
        return new QiniuLister(new BucketManager(Auth.create(accessKey, secretKey), configuration.clone()), bucket,
                prefix, marker, end, unitLen);
    }
}
