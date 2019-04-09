package com.qiniu.datasource;

import com.qiniu.common.SuitsException;
import com.qiniu.convert.QOSObjToMap;
import com.qiniu.convert.QOSObjToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.*;

public class QiniuOssContainer extends OssContainer<FileInfo> {

    private String accessKey;
    private String secretKey;
    private Configuration configuration;

    public QiniuOssContainer(String accessKey, String secretKey, Configuration configuration, String bucket,
                             List<String> antiPrefixes, Map<String, String[]> prefixesMap, boolean prefixLeft,
                             boolean prefixRight, Map<String, String> indexMap, int unitLen, int threads) {
        super(bucket, antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.configuration = configuration;
    }

    @Override
    protected ITypeConvert<FileInfo, Map<String, String>> getNewMapConverter() {
        return new QOSObjToMap(indexMap);
    }

    @Override
    protected ITypeConvert<FileInfo, String> getNewStringConverter() throws IOException {
        return new QOSObjToString(saveFormat, saveSeparator, rmFields);
    }

    @Override
    protected ILister<FileInfo> generateLister(String prefix) throws SuitsException {
        int retry = retryTimes;
        while (true) {
            try {
                String[] markerAndEnd = getMarkerAndEnd(prefix);
                return new QiniuLister(new BucketManager(Auth.create(accessKey, secretKey), configuration.clone()),
                        bucket, prefix, markerAndEnd[0], markerAndEnd[1], null, unitLen);
            } catch (SuitsException e) {
                System.out.println("list prefix:" + prefix + " retrying...");
                if (HttpResponseUtils.checkStatusCode(e.getStatusCode()) < 0) throw e;
                else if (retry <= 0 && e.getStatusCode() >= 500) throw e;
                else retry--;
            }
        }
    }
}
