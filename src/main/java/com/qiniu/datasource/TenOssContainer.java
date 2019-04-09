package com.qiniu.datasource;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.model.COSObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.COSObjToMap;
import com.qiniu.convert.COSObjToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.*;

public class TenOssContainer extends OssContainer<COSObjectSummary> {

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
    protected ITypeConvert<COSObjectSummary, Map<String, String>> getNewMapConverter() {
        return new COSObjToMap(indexMap);
    }

    @Override
    protected ITypeConvert<COSObjectSummary, String> getNewStringConverter() throws IOException {
        return new COSObjToString(saveFormat, saveSeparator, rmFields);
    }

    @Override
    protected ILister<COSObjectSummary> generateLister(String prefix) throws SuitsException {
        int retry = retryTimes;
        while (true) {
            try {
                String[] markerAndEnd = getMarkerAndEnd(prefix);
                return new TenLister(new COSClient(new BasicCOSCredentials(secretId, secretKey), clientConfig),
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
