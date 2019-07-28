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
import com.qiniu.util.LineUtils;

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
                           boolean prefixRight, Map<String, String> indexMap, int unitLen, int threads) throws SuitsException {
        super(bucket, antiPrefixes, prefixesMap, prefixLeft, prefixRight, indexMap, unitLen, threads);
        this.secretId = secretId;
        this.secretKey = secretKey;
        this.clientConfig = clientConfig;
        TenLister tenLister = new TenLister(new COSClient(new BasicCOSCredentials(secretId, secretKey), clientConfig),
                bucket, null, null, null, 1);
        tenLister.close();
        tenLister = null;
        indexPair = LineUtils.getReversedIndexMap(indexMap, rmFields);
        for (String mimeField : LineUtils.mimeFields) indexPair.remove(mimeField);
        for (String statusField : LineUtils.statusFields) indexPair.remove(statusField);
        for (String md5Field : LineUtils.md5Fields) indexPair.remove(md5Field);
        fields = new ArrayList<>();
        for (String defaultFileField : LineUtils.defaultFileFields) {
            if (indexPair.containsKey(defaultFileField)) fields.add(defaultFileField);
        }
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
                return LineUtils.toPair(line, indexPair, new StringMapPair());
            }
        };
    }

    @Override
    protected ITypeConvert<COSObjectSummary, String> getNewStringConverter() throws IOException {
        IStringFormat<COSObjectSummary> stringFormatter;
        if ("json".equals(saveFormat)) {
            stringFormatter = line -> LineUtils.toPair(line, indexPair, new JsonObjectPair()).toString();
        } else if ("csv".equals(saveFormat)) {
            stringFormatter = line -> LineUtils.toFormatString(line, ",", fields);
        } else if ("tab".equals(saveFormat)) {
            stringFormatter = line -> LineUtils.toFormatString(line, saveSeparator, fields);
        } else {
            throw new IOException("please check your format for map to string.");
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
