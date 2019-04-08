package com.qiniu.datasource;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.model.COSObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.COSObjectToString;
import com.qiniu.interfaces.ITypeConvert;

import java.io.IOException;
import java.util.*;

public class TenOssContainer extends OssContainer<COSObjectSummary> {

    private String secretId;
    private String secretKey;
    private ClientConfig clientConfig;

    public TenOssContainer(String secretId, String secretKey, ClientConfig clientConfig, String bucket,
                           List<String> antiPrefixes, Map<String, String[]> prefixesMap, boolean prefixLeft,
                           boolean prefixRight, Map<String, String> indexMap, int unitLen, int threads, String savePath) {
        super(bucket, antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads, savePath);
        this.secretId = secretId;
        this.secretKey = secretKey;
        this.clientConfig = clientConfig;
    }

    @Override
    protected ITypeConvert<COSObjectSummary, Map<String, String>> getNewMapConverter() throws IOException {
        return null;
    }

    @Override
    protected ITypeConvert<COSObjectSummary, String> getNewStringConverter() throws IOException {
        return new COSObjectToString(saveFormat, saveSeparator, rmFields);
    }

    @Override
    protected ILister<COSObjectSummary> generateLister(String prefix) throws SuitsException {
        TenLister tenLister;
        int retry = retryTimes + 1;
        while (true) {
            try {
                tenLister = new TenLister(new COSClient(new BasicCOSCredentials(secretId, secretKey), clientConfig),
                        bucket, prefix, getMarkerAndEnd(prefix)[0], getMarkerAndEnd(prefix)[1], null, unitLen);
                break;
            } catch (CosClientException e) {
                System.out.println("list prefix:" + prefix + "\tmay be retrying...");
//                retry = HttpResponseUtils.checkException(e, retry);
                if (retry == -2) throw e; // 只有当重试次数用尽且响应状态码为 599 时才会抛出异常
            }
        }
        return tenLister;
    }
}
