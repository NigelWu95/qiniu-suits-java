package com.qiniu.datasource;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.model.COSObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.Converter;
import com.qiniu.convert.JsonObjectPair;
import com.qiniu.convert.StringMapPair;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.util.CloudAPIUtils;
import com.qiniu.util.ConvertingUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

public class TenCosContainer extends CloudStorageContainer<COSObjectSummary, BufferedWriter, Map<String, String>> {

    private String secretId;
    private String secretKey;
    private ClientConfig clientConfig;
    private Map<String, String> indexPair;
    private List<String> fields;

    public TenCosContainer(String secretId, String secretKey, ClientConfig clientConfig, String bucket,
                           List<String> antiPrefixes, Map<String, Map<String, String>> prefixesMap, boolean prefixLeft,
                           boolean prefixRight, Map<String, String> indexMap, int unitLen, int threads) throws IOException {
        super(bucket, antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        this.secretId = secretId;
        this.secretKey = secretKey;
        this.clientConfig = clientConfig;
        TenLister tenLister = new TenLister(new COSClient(new BasicCOSCredentials(secretId, secretKey), clientConfig),
                bucket, null, null, null, 1);
        tenLister.close();
        tenLister = null;
    }

    @Override
    public String getSourceName() {
        return "tencent";
    }

    @Override
    protected ITypeConvert<COSObjectSummary, Map<String, String>> getNewConverter() {
        return new Converter<COSObjectSummary, Map<String, String>>() {
            @Override
            public Map<String, String> convertToV(COSObjectSummary line) throws IOException {
                return ConvertingUtils.toPair(line, indexMap, new StringMapPair());
            }
        };
    }

    @Override
    protected ITypeConvert<COSObjectSummary, String> getNewStringConverter() {
        IStringFormat<COSObjectSummary> stringFormatter;
        if (indexPair == null) {
            indexPair = ConvertingUtils.getReversedIndexMap(indexMap, rmFields);
            for (String mimeField : ConvertingUtils.mimeFields) indexPair.remove(mimeField);
            for (String statusField : ConvertingUtils.statusFields) indexPair.remove(statusField);
            for (String md5Field : ConvertingUtils.md5Fields) indexPair.remove(md5Field);
        }
        if ("json".equals(saveFormat)) {
            stringFormatter = line -> ConvertingUtils.toPair(line, indexPair, new JsonObjectPair()).toString();
        } else {
            if (fields == null) fields = ConvertingUtils.getKeyOrderFields(indexPair);
            stringFormatter = line -> ConvertingUtils.toFormatString(line, saveSeparator, fields);
        }
        return new Converter<COSObjectSummary, String>() {
            @Override
            public String convertToV(COSObjectSummary line) throws IOException {
                return stringFormatter.toFormatString(line);
            }
        };
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected ILister<COSObjectSummary> getLister(String prefix, String marker, String start, String end) throws SuitsException {
        if (marker == null || "".equals(marker)) marker = CloudAPIUtils.getTenCosMarker(start);
        return new TenLister(new COSClient(new BasicCOSCredentials(secretId, secretKey), clientConfig), bucket, prefix,
                marker, end, unitLen);
    }
}
