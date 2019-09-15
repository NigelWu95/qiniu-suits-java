package com.qiniu.datasource;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.interfaces.ILister;

import java.util.ArrayList;
import java.util.List;

public class AwsS3Lister implements ILister<S3ObjectSummary> {

    private AmazonS3 s3Client;
    private ListObjectsV2Request listObjectsRequest;
    private String endPrefix;
    private List<S3ObjectSummary> s3ObjectList;
    private long count;
    private static final List<S3ObjectSummary> defaultList = new ArrayList<>();

    public AwsS3Lister(AmazonS3 s3Client, String bucket, String prefix, String marker, String start, String endPrefix,
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
        count += s3ObjectList.size();
    }

    @Override
    public String getBucket() {
        return listObjectsRequest.getBucketName();
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
        count -= s3ObjectList.size();
        checkedListWithEnd();
        count += s3ObjectList.size();
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

    private void checkedListWithEnd() {
        if (endPrefix == null || "".equals(endPrefix)) return;
        String endKey = currentEndKey();
        if (endKey == null) return;
        if (endKey.compareTo(endPrefix) == 0) {
            listObjectsRequest.setContinuationToken(null);
            if (endPrefix.equals(getPrefix() + CloudStorageContainer.firstPoint)) {
                s3ObjectList.remove(s3ObjectList.size() - 1);
//                if (s3ObjectList.size() > 0) {
//                    int lastIndex = s3ObjectList.size() - 1;
//                    S3ObjectSummary last = s3ObjectList.get(lastIndex);
//                    if (endPrefix.equals(last.getKey()))
//                        s3ObjectList.remove(lastIndex);
//                }
            }
        } else if (endKey.compareTo(endPrefix) > 0) {
            listObjectsRequest.setContinuationToken(null);
            int size = s3ObjectList.size();
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            for (int i = 0; i < size; i++) {
                if (s3ObjectList.get(i).getKey().compareTo(endPrefix) > 0) {
                    s3ObjectList = s3ObjectList.subList(0, i);
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
        } catch (SdkClientException e) {
            throw new SuitsException(-1, e.getMessage());
        } catch (NullPointerException e) {
            throw new SuitsException(e, 400000, "lister maybe already closed");
        } catch (Exception e) {
            throw new SuitsException(e, -1, "listing failed");
        }
    }

    @Override
    public synchronized void listForward() throws SuitsException {
        if (hasNext()) {
            doList();
            count += s3ObjectList.size();
        } else {
            s3ObjectList = defaultList;
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
            count += s3ObjectList.size();
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
    public String currentEndKey() {
        if (s3ObjectList.size() > 0) return s3ObjectList.get(s3ObjectList.size() - 1).getKey();
        return null;
    }

    @Override
    public synchronized String truncate() {
        String truncateMarker = null;
        if (hasNext()) {
            truncateMarker = listObjectsRequest.getContinuationToken();
            listObjectsRequest.setContinuationToken(null);
        }
        return truncateMarker;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public void close() {
        s3Client.shutdown();
//        listObjectsRequest = null;
        endPrefix = null;
        s3ObjectList = defaultList;
    }
}
