package com.qiniu.datasource;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.OSSObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.OSSObjToMap;
import com.qiniu.convert.OSSObjToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.util.CloudAPIUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AliOssContainer extends CloudStorageContainer<OSSObjectSummary, BufferedWriter, Map<String, String>> {

    private String accessKeyId;
    private String accessKeySecret;
    private ClientConfiguration clientConfig;
    private String endpoint;

    public AliOssContainer(String accessKeyId, String accessKeySecret, ClientConfiguration clientConfig, String endpoint,
                           String bucket, List<String> antiPrefixes, Map<String, Map<String, String>> prefixesMap,
                           boolean prefixLeft, boolean prefixRight, Map<String, String> indexMap, int unitLen, int threads)
            throws SuitsException {
        super(bucket, antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.clientConfig = clientConfig;
        this.endpoint = endpoint;
        AliLister aliLister = new AliLister(new OSSClient(endpoint, new DefaultCredentialProvider(accessKeyId, accessKeySecret),
                clientConfig), bucket, null, null, null, 1);
        aliLister.close();
        aliLister = null;
    }

    @Override
    public String getSourceName() {
        return "aliyun";
    }

    @Override
    protected ITypeConvert<OSSObjectSummary, Map<String, String>> getNewConverter() {
        return new OSSObjToMap(indexMap);
    }

    @Override
    protected ITypeConvert<OSSObjectSummary, String> getNewStringConverter() throws IOException {
        return new OSSObjToString(saveFormat, saveSeparator, rmFields);
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected ILister<OSSObjectSummary> getLister(String prefix, String marker, String start, String end) throws SuitsException {
        if (marker == null || "".equals(marker)) marker = CloudAPIUtils.getAliOssMarker(start);
        return new AliLister(new OSSClient(endpoint, new DefaultCredentialProvider(accessKeyId, accessKeySecret),
                clientConfig), bucket, prefix, marker, end, unitLen);
    }
}
