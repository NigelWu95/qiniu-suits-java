package com.qiniu.datasource;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qiniu.common.SuitsException;
import com.qiniu.interfaces.ILister;
import com.qiniu.util.CloudApiUtils;

import java.util.List;

public class TenLister implements ILister<COSObjectSummary> {

    private COSClient cosClient;
    private ListObjectsRequest listObjectsRequest;
    private String endPrefix;
    private String truncateMarker;
    private List<COSObjectSummary> cosObjectList;
    private String endKey;
    private long count;

    public TenLister(COSClient cosClient, String bucket, String prefix, String marker, String endPrefix, int max) throws SuitsException {
        this.cosClient = cosClient;
        this.listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        listObjectsRequest.setPrefix(prefix);
        listObjectsRequest.setMarker("".equals(marker) ? null : marker);
        listObjectsRequest.setMaxKeys(max);
        this.endPrefix = endPrefix;
        doList();
        count += cosObjectList.size();
    }

    @Override
    public String getBucket() {
        return listObjectsRequest.getBucketName();
    }

    public String getPrefix() {
        return listObjectsRequest.getPrefix();
    }

    public void setMarker(String marker) {
        listObjectsRequest.setMarker("".equals(marker) ? null : marker);
    }

    public String getMarker() {
        return listObjectsRequest.getMarker();
    }

    @Override
    public void setEndPrefix(String endPrefix) {
        this.endPrefix = endPrefix;
        count -= cosObjectList.size();
        checkedListWithEnd();
        count += cosObjectList.size();
    }

    @Override
    public String getEndPrefix() {
        return endPrefix;
    }

    @Override
    public void setLimit(int limit) {
        listObjectsRequest.setMaxKeys(limit);
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
            listObjectsRequest.setMarker(null);
            if (endPrefix.equals(getPrefix() + CloudStorageContainer.firstPoint) && cosObjectList.size() > 0) {
                int lastIndex = cosObjectList.size() - 1;
                if (endPrefix.equals(cosObjectList.get(lastIndex).getKey())) cosObjectList.remove(lastIndex);
            }
        } else if (endKey.compareTo(endPrefix) > 0) {
            listObjectsRequest.setMarker(null);
            int size = cosObjectList.size();
            int i = 0;
            for (; i < size; i++) {
                if (cosObjectList.get(i).getKey().compareTo(endPrefix) > 0) {
//                    cosObjectList.remove(i);
                    break;
                }
            }
            // 优化 gc，不用的元素全部清除
            cosObjectList.subList(i, size).clear();
        }
    }

    private void doList() throws SuitsException {
        try {
            ObjectListing objectListing = cosClient.listObjects(listObjectsRequest);
            listObjectsRequest.setMarker(objectListing.getNextMarker());
            cosObjectList = objectListing.getObjectSummaries();
            checkedListWithEnd();
        } catch (CosServiceException e) {
            throw new SuitsException(e,e.getStatusCode());
        } catch (CosClientException e) {
            throw new SuitsException(e, -1);
        } catch (NullPointerException e) {
            throw new SuitsException(e, 400000, "lister maybe already closed");
        } catch (Exception e) {
            throw new SuitsException(e, -1, "listing failed");
        }
    }

    @Override
    public synchronized void listForward() throws SuitsException {
        if (hasNext()) {
            cosObjectList.clear();
            doList();
            count += cosObjectList.size();
        } else {
            if (cosObjectList.size() > 0) {
                endKey = cosObjectList.get(cosObjectList.size() - 1).getKey();
                cosObjectList.clear();
            }
        }
    }

    @Override
    public boolean hasNext() {
        return listObjectsRequest.getMarker() != null && !"".equals(listObjectsRequest.getMarker());
    }

    @Override
    public boolean hasFutureNext() throws SuitsException {
        int expected = listObjectsRequest.getMaxKeys() + 1;
        if (expected <= 10000) expected = 10001;
        int times = 10;
        List<COSObjectSummary> futureList = CloudApiUtils.initFutureList(listObjectsRequest.getMaxKeys(), times);
        futureList.addAll(cosObjectList);
        cosObjectList.clear();
        SuitsException exception = null;
        while (futureList.size() < expected && times > 0 && hasNext()) {
            times--;
            try {
                doList();
                count += cosObjectList.size();
                futureList.addAll(cosObjectList);
                cosObjectList.clear();
            } catch (SuitsException e) {
//                cosObjectList = futureList;
//                throw e;
                exception = e;
            }
        }
        cosObjectList = futureList;
        futureList = null;
        if (exception != null) throw exception;
        return hasNext();
    }

    @Override
    public List<COSObjectSummary> currents() {
        return cosObjectList;
    }

    @Override
    public synchronized String currentEndKey() {
        if (hasNext()) return getMarker();
        if (truncateMarker != null && !"".equals(truncateMarker)) return truncateMarker;
        if (endKey != null) return endKey;
        if (cosObjectList.size() > 0) return cosObjectList.get(cosObjectList.size() - 1).getKey();
        return null;
    }

    @Override
    public synchronized String truncate() {
        truncateMarker = listObjectsRequest.getMarker();
        listObjectsRequest.setMarker(null);
        return truncateMarker;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public void close() {
        cosClient.shutdown();
//        listObjectsRequest = null;
        endPrefix = null;
        if (cosObjectList.size() > 0) {
            COSObjectSummary last = cosObjectList.get(cosObjectList.size() - 1);
            if (last != null) endKey = last.getKey();
            cosObjectList.clear();
        }
    }
}
