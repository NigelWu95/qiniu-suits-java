package com.qiniu.datasource;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.qiniu.common.SuitsException;
import com.qiniu.interfaces.ILister;
import com.qiniu.util.CloudApiUtils;

import java.util.List;

public class AwsS3Lister implements ILister<S3ObjectSummary> {

    private AmazonS3 s3Client;
    private ListObjectsV2Request listObjectsRequest;
    private String endPrefix;
    private List<S3ObjectSummary> s3ObjectList;
    private long count;
    private String endKey;

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
        listObjectsRequest.setStartAfter(null); // 做完一次 list 之后该值应当失效，直接在此处置为空
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
            if (endPrefix.equals(getPrefix() + CloudStorageContainer.firstPoint) && s3ObjectList.size() > 0) {
                int lastIndex = s3ObjectList.size() - 1;
                if (endPrefix.equals(s3ObjectList.get(lastIndex).getKey())) s3ObjectList.remove(lastIndex);
            }
        } else if (endKey.compareTo(endPrefix) > 0) {
            listObjectsRequest.setContinuationToken(null);
            int size = s3ObjectList.size();
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            int i = 0;
            for (; i < size; i++) {
                if (s3ObjectList.get(i).getKey().compareTo(endPrefix) > 0) {
//                    s3ObjectList.remove(i);
                    break;
                }
            }
            // 优化 gc，不用的元素全部清除
            s3ObjectList.subList(i, size).clear();
        }
    }

    private void doList() throws SuitsException {
        try {
            ListObjectsV2Result result = s3Client.listObjectsV2(listObjectsRequest);
            listObjectsRequest.setContinuationToken(result.getNextContinuationToken());
            s3ObjectList = result.getObjectSummaries();
            if (s3ObjectList.size() > 0) {
                endKey = s3ObjectList.get(s3ObjectList.size() - 1).getKey();
            }
            checkedListWithEnd();
        } catch (AmazonServiceException e) {
            throw new SuitsException(e, e.getStatusCode());
        } catch (SdkClientException e) {
            throw new SuitsException(e, -1);
        } catch (NullPointerException e) {
            throw new SuitsException(e, 400000, "lister maybe already closed");
        } catch (Exception e) {
            throw new SuitsException(e, -1, "listing failed");
        }
    }

    @Override
    public synchronized void listForward() throws SuitsException {
        s3ObjectList.clear();
        if (hasNext()) {
            doList();
            count += s3ObjectList.size();
        }
    }

    @Override
    public boolean hasNext() {
        return listObjectsRequest.getContinuationToken() != null && !"".equals(listObjectsRequest.getContinuationToken());
    }

    @Override
    public boolean hasFutureNext() throws SuitsException {
        int expected = listObjectsRequest.getMaxKeys() + 1;
        if (expected <= 10000) expected = 10001;
        int times = 10;
        List<S3ObjectSummary> futureList = CloudApiUtils.initFutureList(listObjectsRequest.getMaxKeys(), times);
        futureList.addAll(s3ObjectList);
        s3ObjectList.clear();
        while (futureList.size() < expected && times > 0 && hasNext()) {
            times--;
            try {
                doList();
                count += s3ObjectList.size();
                futureList.addAll(s3ObjectList);
                s3ObjectList.clear();
            } catch (SuitsException e) {
                s3ObjectList = futureList;
                throw e;
            }
        }
        s3ObjectList = futureList;
        futureList = null;
        return hasNext();
    }

    @Override
    public List<S3ObjectSummary> currents() {
        return s3ObjectList;
    }

    @Override
    public String currentEndKey() {
        return endKey;
    }

    @Override
    public synchronized String truncate() {
        String truncateMarker = listObjectsRequest.getContinuationToken();
        listObjectsRequest.setContinuationToken(null);
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
        s3ObjectList.clear();
    }
}
