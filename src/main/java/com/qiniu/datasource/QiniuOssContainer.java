package com.qiniu.datasource;

import com.qiniu.common.SuitsException;
import com.qiniu.convert.FileInfoToMap;
import com.qiniu.convert.FileInfoToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;

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
    protected ITypeConvert<FileInfo, Map<String, String>> getNewMapConverter() throws IOException {
        return new FileInfoToMap(indexMap);
    }

    @Override
    protected ITypeConvert<FileInfo, String> getNewStringConverter() throws IOException {
        return new FileInfoToString(saveFormat, saveSeparator, rmFields);
    }

    @Override
    protected ILister<FileInfo> generateLister(String prefix) throws SuitsException {
        QiniuLister fileLister;
        int retry = retryTimes + 1;
        while (true) {
            try {
                fileLister = new QiniuLister(new BucketManager(Auth.create(accessKey, secretKey), configuration.clone()),
                        bucket, prefix, getMarkerAndEnd(prefix)[0], getMarkerAndEnd(prefix)[1], null, unitLen);
                break;
            } catch (SuitsException e) {
                System.out.println("list prefix:" + prefix + "\tmay be retrying...");
//                retry = HttpResponseUtils.checkException(e, retry);
                if (retry == -2) throw e; // 只有当重试次数用尽且响应状态码为 599 时才会抛出异常
            }
        }
        return fileLister;
    }
}
