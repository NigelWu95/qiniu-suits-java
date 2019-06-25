package com.qiniu.datasource;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.util.HttpRespUtils;

import java.util.List;

public class S3Lister implements ILister<S3ObjectSummary> {

    private AmazonS3 s3Client;
    private ListObjectsV2Request listObjectsRequest;
    private String endPrefix;
    private boolean straight;
    private List<S3ObjectSummary> s3ObjectSummaryList;

    public S3Lister(AmazonS3 s3Client, String bucket, String prefix, String marker, String endPrefix, String delimiter,
                    int max) throws SuitsException {
        this.s3Client = s3Client;
        this.listObjectsRequest = new ListObjectsV2Request();
        listObjectsRequest.setBucketName(bucket);
        listObjectsRequest.setPrefix(prefix);
        listObjectsRequest.setDelimiter(delimiter);
        listObjectsRequest.setContinuationToken("".equals(marker) ? null : marker);
        listObjectsRequest.setMaxKeys(max);
        this.endPrefix = endPrefix;
        doList();
    }

    @Override
    public void setPrefix(String prefix) {
        listObjectsRequest.setPrefix(prefix);
    }

    @Override
    public String getPrefix() {
        return listObjectsRequest.getPrefix();
    }

    @Override
    public void setMarker(String marker) {
        listObjectsRequest.setContinuationToken("".equals(marker) ? null : marker);
    }

    @Override
    public String getMarker() {
        return listObjectsRequest.getContinuationToken();
    }

    @Override
    public void setEndPrefix(String endPrefix) {
        this.endPrefix = endPrefix;
        checkedListWithEnd();
    }

    @Override
    public String getEndPrefix() {
        return endPrefix;
    }

    @Override
    public void setDelimiter(String delimiter) {
        listObjectsRequest.setDelimiter(delimiter);
    }

    @Override
    public String getDelimiter() {
        return listObjectsRequest.getDelimiter();
    }

    @Override
    public void setLimit(int limit) {
        listObjectsRequest.withMaxKeys(limit);
    }

    @Override
    public int getLimit() {
        return listObjectsRequest.getMaxKeys();
    }

    @Override
    public void setStraight(boolean straight) {
        this.straight = straight;
    }

    @Override
    public boolean getStraight() {
        return straight;
    }

    @Override
    public boolean canStraight() {
        return straight || !hasNext() || (endPrefix != null && !"".equals(endPrefix));
    }

    private void checkedListWithEnd() {
        String endKey = currentEndKey();
        if (endPrefix != null && !"".equals(endPrefix) && endKey != null && endKey.compareTo(endPrefix) >= 0) {
            int size = s3ObjectSummaryList.size();
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            for (int i = 0; i < size; i++) {
                if (s3ObjectSummaryList.get(i).getKey().compareTo(endPrefix) > 0) {
                    s3ObjectSummaryList = s3ObjectSummaryList.subList(0, i);
                    listObjectsRequest.setContinuationToken(null);
                    return;
                }
            }
        }
    }

    private void doList() throws SuitsException {
        try {
            ListObjectsV2Result result = s3Client.listObjectsV2(listObjectsRequest);
            listObjectsRequest.setContinuationToken(result.getNextContinuationToken());
            listObjectsRequest.setStartAfter(null);
            s3ObjectSummaryList = result.getObjectSummaries();
            checkedListWithEnd();
        } catch (AmazonServiceException e) {
            throw new SuitsException(e.getStatusCode(), e.getMessage());
        } catch (NullPointerException e) {
            throw new SuitsException(400000, "lister maybe already closed, " + e.getMessage());
        } catch (Exception e) {
            throw new SuitsException(-1, "failed, " + e.getMessage());
        }
    }

    @Override
    public void listForward() throws SuitsException {
        if (hasNext()) {
            doList();
        } else {
            s3ObjectSummaryList.clear();
        }
    }

    @Override
    public boolean hasNext() {
        return (listObjectsRequest.getContinuationToken() != null && !"".equals(listObjectsRequest.getContinuationToken()))
                || (listObjectsRequest.getStartAfter() != null && !"".equals(listObjectsRequest.getStartAfter()));
    }

    @Override
    public boolean hasFutureNext() throws SuitsException {
        int times = 50000 / (s3ObjectSummaryList.size() + 1);
        times = times > 10 ? 10 : times;
        List<S3ObjectSummary> futureList = s3ObjectSummaryList;
        while (hasNext() && times > 0 && futureList.size() < 10001) {
            if (futureList.size() > 0) times--;
            doList();
            futureList.addAll(s3ObjectSummaryList);
        }
        s3ObjectSummaryList = futureList;
        return hasNext();
    }

    @Override
    public List<S3ObjectSummary> currents() {
        return s3ObjectSummaryList;
    }

    @Override
    public S3ObjectSummary currentLast() {
        return s3ObjectSummaryList.size() > 0 ? s3ObjectSummaryList.get(s3ObjectSummaryList.size() - 1) : null;
    }

    @Override
    public String currentEndKey() {
        S3ObjectSummary last = currentLast();
        if (last != null) {
            return last.getKey();
        } else {
            int retry = 5;
            while (true) {
                try {
                    hasFutureNext();
                    break;
                } catch (SuitsException e) {
                    retry--;
                    if (HttpRespUtils.checkStatusCode(e.getStatusCode()) < 0 || (retry <= 0 && e.getStatusCode() >= 500))
                        throw new RuntimeException("get currentEndKey failed, " + e.getMessage());
                }
            }
            last = currentLast();
            return last != null ? last.getKey() : null;
        }
    }

    @Override
    public void updateMarkerBy(S3ObjectSummary object) {
        if (object != null) {
            listObjectsRequest.setContinuationToken(null);
            listObjectsRequest.setStartAfter(object.getKey());
        }
    }

    @Override
    public void close() {
        s3Client.shutdown();
        s3ObjectSummaryList = null;
    }
}
