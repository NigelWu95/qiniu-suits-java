package com.qiniu.datasource;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qiniu.common.SuitsException;
import com.qiniu.util.ListingUtils;

import java.util.List;

public class TenLister implements ILister<COSObjectSummary> {

    private COSClient cosClient;
    private ListObjectsRequest listObjectsRequest;
    private String endPrefix;
    private List<COSObjectSummary> cosObjectList;

    public TenLister(COSClient cosClient, String bucket, String prefix, String marker, String endPrefix, int max) throws SuitsException {
        this.cosClient = cosClient;
        this.listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        listObjectsRequest.setPrefix(prefix);
        listObjectsRequest.setMarker("".equals(marker) ? null : marker);
        listObjectsRequest.setMaxKeys(max);
        this.endPrefix = endPrefix;
        doList();
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
        checkedListWithEnd();
    }

    @Override
    public String getEndPrefix() {
        return endPrefix;
    }

    @Override
    public void setLimit(int limit) {
        listObjectsRequest.setMaxKeys(limit);
    }

    public int getLimit() {
        return listObjectsRequest.getMaxKeys();
    }

    private void checkedListWithEnd() {
        String endKey = currentEndKey();
        if (endPrefix == null || "".equals(endPrefix) || endKey == null) return;
        if (endKey.compareTo(endPrefix) == 0) {
            listObjectsRequest.setMarker(null);
            if (endPrefix.equals(getPrefix() + CloudStorageContainer.startPoint)) {
                COSObjectSummary last = currentLast();
                if (last != null && endPrefix.equals(last.getKey()))
                    cosObjectList.remove(last);
            }
        } else if (endKey.compareTo(endPrefix) > 0) {
            listObjectsRequest.setMarker(null);
            int size = cosObjectList.size();
            for (int i = 0; i < size; i++) {
                if (cosObjectList.get(i).getKey().compareTo(endPrefix) > 0) {
                    cosObjectList = cosObjectList.subList(0, i);
                    return;
                }
            }
        }
    }

    private void doList() throws SuitsException {
        try {
            ObjectListing objectListing = cosClient.listObjects(listObjectsRequest);
            listObjectsRequest.setMarker(objectListing.getNextMarker());
            cosObjectList = objectListing.getObjectSummaries();
            checkedListWithEnd();
        } catch (CosServiceException e) {
            throw new SuitsException(e, e.getStatusCode());
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
        } else {
            cosObjectList.clear();
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
        int times = 100000 / (cosObjectList.size() + 1) + 1;
        times = times > 10 ? 10 : times;
        List<COSObjectSummary> futureList = cosObjectList;
        while (hasNext() && times > 0 && futureList.size() < expected) {
            times--;
            doList();
            futureList.addAll(cosObjectList);
        }
        cosObjectList = futureList;
        return hasNext();
    }

    @Override
    public synchronized List<COSObjectSummary> currents() {
        return cosObjectList;
    }

    @Override
    public COSObjectSummary currentLast() {
        return cosObjectList.size() > 0 ? cosObjectList.get(cosObjectList.size() - 1) : null;
    }

    @Override
    public String currentEndKey() {
        if (hasNext()) return getMarker();
        COSObjectSummary last = currentLast();
        return last != null ? last.getKey() : null;
    }

    @Override
    public void updateMarkerBy(COSObjectSummary object) {
        if (object != null) listObjectsRequest.setMarker(ListingUtils.getTenCosMarker(object.getKey()));
    }

    @Override
    public synchronized String truncate() {
        String endKey = currentEndKey();
        setEndPrefix(endKey);
        return endKey;
    }

    @Override
    public void close() {
        cosClient.shutdown();
        listObjectsRequest = null;
        endPrefix = null;
        cosObjectList.clear();
    }
}
