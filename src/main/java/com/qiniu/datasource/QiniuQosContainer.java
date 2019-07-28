package com.qiniu.datasource;

import com.qiniu.common.SuitsException;
import com.qiniu.convert.Converter;
import com.qiniu.convert.JsonObjectPair;
import com.qiniu.convert.StringMapPair;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.CloudAPIUtils;
import com.qiniu.util.LineUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

public class QiniuQosContainer extends CloudStorageContainer<FileInfo, BufferedWriter, Map<String, String>> {

    private String accessKey;
    private String secretKey;
    private Configuration configuration;
    private Map<String, String> indexPair;
    private List<String> fields;

    public QiniuQosContainer(String accessKey, String secretKey, Configuration configuration, String bucket,
                             List<String> antiPrefixes, Map<String, Map<String, String>> prefixesMap, boolean prefixLeft,
                             boolean prefixRight, Map<String, String> indexMap, int unitLen, int threads) throws IOException {
        super(bucket, antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
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
                return LineUtils.toPair(line, indexMap, new StringMapPair());
            }
        };
    }

    @Override
    protected ITypeConvert<FileInfo, String> getNewStringConverter() {
        IStringFormat<FileInfo> stringFormatter;
        if (indexPair == null) indexPair = LineUtils.getReversedIndexMap(indexMap, rmFields);
        if ("json".equals(saveFormat)) {
            stringFormatter = line -> LineUtils.toPair(line, indexPair, new JsonObjectPair()).toString();
        } else {
            if (fields == null) fields = LineUtils.getValueFields(indexPair);
            stringFormatter = line -> LineUtils.toFormatString(line, saveSeparator, fields);
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
