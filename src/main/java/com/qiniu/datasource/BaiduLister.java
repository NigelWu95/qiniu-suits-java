package com.qiniu.datasource;

import com.baidubce.BceClientException;
import com.baidubce.BceServiceException;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.qiniu.common.SuitsException;
import com.qiniu.interfaces.ILister;
import com.qiniu.util.CloudApiUtils;

import java.util.ArrayList;
import java.util.List;

public class BaiduLister implements ILister<BosObjectSummary> {

    private BosClient bosClient;
    private ListObjectsRequest listObjectsRequest;
    private String endPrefix;
    private List<BosObjectSummary> bosObjectList;
    private static final List<BosObjectSummary> defaultList = new ArrayList<>();

    public BaiduLister(BosClient bosClient, String bucket, String prefix, String marker, String endPrefix, int max) throws SuitsException {
        this.bosClient = bosClient;
        this.listObjectsRequest = new ListObjectsRequest(bucket);
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
        if (endPrefix == null || "".equals(endPrefix)) return;
        String endKey = currentEndKey();
        if (endKey == null) return;
        if (endKey.compareTo(endPrefix) == 0) {
            listObjectsRequest.setMarker(null);
            if (endPrefix.equals(getPrefix() + CloudStorageContainer.firstPoint)) {
                if (bosObjectList.size() > 0) {
                    int lastIndex = bosObjectList.size() - 1;
                    BosObjectSummary last = bosObjectList.get(lastIndex);
                    if (endPrefix.equals(last.getKey())) bosObjectList.remove(lastIndex);
                }
            }
        } else if (endKey.compareTo(endPrefix) > 0) {
            listObjectsRequest.setMarker(null);
            int size = bosObjectList.size();
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            for (int i = 0; i < size; i++) {
                if (bosObjectList.get(i).getKey().compareTo(endPrefix) > 0) {
                    bosObjectList = bosObjectList.subList(0, i);
                    return;
                }
            }
        }
    }

    private void doList() throws SuitsException {
        try {
            ListObjectsResponse objectListing = bosClient.listObjects(listObjectsRequest);
            listObjectsRequest.setMarker(objectListing.getNextMarker());
            bosObjectList = objectListing.getContents();
            checkedListWithEnd();
        } catch (BceServiceException e) {
            throw new SuitsException(e, CloudApiUtils.AliStatusCode(e.getErrorCode(), -1));
        } catch (BceClientException e) {
            throw new SuitsException(e, -1, e.getMessage());
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
            bosObjectList = defaultList;
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
        int times = 100000 / (bosObjectList.size() + 1) + 1;
        times = times > 10 ? 10 : times;
        List<BosObjectSummary> futureList = bosObjectList;
        while (hasNext() && times > 0 && futureList.size() < expected) {
            times--;
            doList();
            futureList.addAll(bosObjectList);
        }
        bosObjectList = futureList;
        return hasNext();
    }

    @Override
    public List<BosObjectSummary> currents() {
        return bosObjectList;
    }

    @Override
    public String currentEndKey() {
        if (hasNext()) return getMarker();
        if (bosObjectList.size() > 0) return bosObjectList.get(bosObjectList.size() - 1).getKey();
        return null;
    }

    @Override
    public synchronized String truncate() {
        String truncateMarker = listObjectsRequest.getMarker();
        listObjectsRequest.setMarker(null);
        return truncateMarker;
    }

    @Override
    public void close() {
        bosClient.shutdown();
//        listObjectsRequest = null;
        endPrefix = null;
        bosObjectList = defaultList;
    }
}
