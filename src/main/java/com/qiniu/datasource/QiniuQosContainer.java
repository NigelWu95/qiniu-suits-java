package com.qiniu.datasource;

import com.qiniu.common.SuitsException;
import com.qiniu.convert.QOSObjToMap;
import com.qiniu.convert.QOSObjToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

public class QiniuQosContainer extends CloudStorageContainer<FileInfo, BufferedWriter, Map<String, String>> {

    private String accessKey;
    private String secretKey;
    private Configuration configuration;

    public QiniuQosContainer(String accessKey, String secretKey, Configuration configuration, String bucket,
                             List<String> antiPrefixes, Map<String, String[]> prefixesMap, boolean prefixLeft,
                             boolean prefixRight, Map<String, String> indexMap, int unitLen, int threads) {
        super(bucket, antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.configuration = configuration;
    }

    @Override
    public String getSourceName() {
        return "qiniu";
    }

    @Override
    protected ITypeConvert<FileInfo, Map<String, String>> getNewConverter() {
        return new QOSObjToMap(indexMap);
    }

    @Override
    protected ITypeConvert<FileInfo, String> getNewStringConverter() throws IOException {
        return new QOSObjToString(saveFormat, saveSeparator, rmFields);
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected ILister<FileInfo> getLister(String prefix, String marker, String end) throws SuitsException {
        return new QiniuLister(new BucketManager(Auth.create(accessKey, secretKey), configuration.clone()), bucket,
                prefix, marker, end, null, unitLen);
    }
}
