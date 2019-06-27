package com.qiniu.datasource;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.qiniu.common.SuitsException;

import java.util.List;

public class S3Lister implements ILister<S3ObjectSummary> {

    private AmazonS3 s3Client;
    private ListObjectsV2Request listObjectsRequest;
    private String endPrefix;
    private boolean straight;
    private List<S3ObjectSummary> s3ObjectList;

    public S3Lister(AmazonS3 s3Client, String bucket, String prefix, String marker, String start, String endPrefix,
                    int max) throws SuitsException {
        this.s3Client = s3Client;
        this.listObjectsRequest = new ListObjectsV2Request();
        listObjectsRequest.setBucketName(bucket);
        listObjectsRequest.setPrefix(prefix);
        listObjectsRequest.setStartAfter(start);
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
            int size = s3ObjectList.size();
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            for (int i = 0; i < size; i++) {
                if (s3ObjectList.get(i).getKey().compareTo(endPrefix) > 0) {
                    s3ObjectList = s3ObjectList.subList(0, i);
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
            s3ObjectList = result.getObjectSummaries();
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
            s3ObjectList.clear();
        }
    }

    @Override
    public boolean hasNext() {
        return (listObjectsRequest.getContinuationToken() != null && !"".equals(listObjectsRequest.getContinuationToken()))
                || (listObjectsRequest.getStartAfter() != null && !"".equals(listObjectsRequest.getStartAfter()));
    }

    @Override
    public boolean hasFutureNext() throws SuitsException {
        int expected = listObjectsRequest.getMaxKeys() + 1;
        if (expected <= 10000) expected = 10001;
        int times = 100000 / (s3ObjectList.size() + 1) + 1;
        times = times > 10 ? 10 : times;
        List<S3ObjectSummary> futureList = s3ObjectList;
        while (hasNext() && times > 0 && futureList.size() < expected) {
            times--;
            doList();
            futureList.addAll(s3ObjectList);
        }
        s3ObjectList = futureList;
        return hasNext();
    }

    @Override
    public List<S3ObjectSummary> currents() {
        return s3ObjectList;
    }

    @Override
    public S3ObjectSummary currentLast() {
        return s3ObjectList.size() > 0 ? s3ObjectList.get(s3ObjectList.size() - 1) : null;
    }

    @Override
    public String currentEndKey() {
//        int retry = 10;
//        while (s3ObjectSummaryList.size() <= 0 && hasNext()) {
//            try {
//                ListObjectsV2Result result = s3Client.listObjectsV2(listObjectsRequest);
//                listObjectsRequest.setContinuationToken(result.getNextContinuationToken());
//                listObjectsRequest.setStartAfter(null);
//                s3ObjectSummaryList = result.getObjectSummaries();
//            } catch (AmazonServiceException e) {
//                if (HttpRespUtils.checkStatusCode(e.getStatusCode()) < 0) ThrowUtils.exit(new AtomicBoolean(true), e);
//            } catch (NullPointerException e) {
//                ThrowUtils.exit(new AtomicBoolean(true), new Exception("lister maybe already closed, " + e.getMessage()));
//            } catch (Exception ignored) {}
//            retry--;
//            if (retry <= 0) break;
//        }
        S3ObjectSummary last = currentLast();
        return last != null ? last.getKey() : null;
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
        s3ObjectList = null;
    }
}
