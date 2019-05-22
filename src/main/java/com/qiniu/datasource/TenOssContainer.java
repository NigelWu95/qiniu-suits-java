package com.qiniu.datasource;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.model.COSObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.COSObjToMap;
import com.qiniu.convert.COSObjToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultOutput;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

public class TenOssContainer extends OssContainer<COSObjectSummary, BufferedWriter, Map<String, String>> {

    private String secretId;
    private String secretKey;
    private ClientConfig clientConfig;

    public TenOssContainer(String secretId, String secretKey, ClientConfig clientConfig, String bucket,
                           List<String> antiPrefixes, Map<String, String[]> prefixesMap, boolean prefixLeft,
                           boolean prefixRight, Map<String, String> indexMap, int unitLen, int threads) {
        super(bucket, antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        this.secretId = secretId;
        this.secretKey = secretKey;
        this.clientConfig = clientConfig;
    }

    @Override
    public String getSourceName() {
        return "tencent";
    }

    @Override
    protected ITypeConvert<COSObjectSummary, Map<String, String>> getNewConverter() {
        return new COSObjToMap(indexMap);
    }

    @Override
    protected ITypeConvert<COSObjectSummary, String> getNewStringConverter() throws IOException {
        return new COSObjToString(saveFormat, saveSeparator, rmFields);
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected ILister<COSObjectSummary> getLister(String prefix, String marker, String end) throws SuitsException {
        return new TenLister(new COSClient(new BasicCOSCredentials(secretId, secretKey), clientConfig), bucket, prefix,
                marker, end, null, unitLen);
    }
}
