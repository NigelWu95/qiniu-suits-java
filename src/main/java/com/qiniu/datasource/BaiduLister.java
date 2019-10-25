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

import java.util.List;

public class BaiduLister implements ILister<BosObjectSummary> {

    private BosClient bosClient;
    private ListObjectsRequest listObjectsRequest;
    private String endPrefix;
    private String truncateMarker;
    private List<BosObjectSummary> bosObjectList;
    private BosObjectSummary last;
    private long count;

    public BaiduLister(BosClient bosClient, String bucket, String prefix, String marker, String endPrefix, int max) throws SuitsException {
        this.bosClient = bosClient;
        this.listObjectsRequest = new ListObjectsRequest(bucket);
        listObjectsRequest.setPrefix(prefix);
        listObjectsRequest.setMarker("".equals(marker) ? null : marker);
        listObjectsRequest.setMaxKeys(max);
        this.endPrefix = endPrefix;
        doList();
        count += bosObjectList.size();
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
        count -= bosObjectList.size();
        checkedListWithEnd();
        count += bosObjectList.size();
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
            if (endPrefix.equals(getPrefix() + CloudStorageContainer.firstPoint) && bosObjectList.size() > 0) {
                int lastIndex = bosObjectList.size() - 1;
                if (endPrefix.equals(bosObjectList.get(lastIndex).getKey())) bosObjectList.remove(lastIndex);
            }
        } else if (endKey.compareTo(endPrefix) > 0) {
            listObjectsRequest.setMarker(null);
            int size = bosObjectList.size();
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            int i = 0;
            for (; i < size; i++) {
                if (bosObjectList.get(i).getKey().compareTo(endPrefix) > 0) {
//                    bosObjectList.remove(i);
                    break;
                }
            }
            // 优化 gc，不用的元素全部清除
            bosObjectList.subList(i, size).clear();
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
            bosObjectList.clear();
            doList();
            count += bosObjectList.size();
        } else {
            if (bosObjectList.size() > 0) {
                last = bosObjectList.get(bosObjectList.size() - 1);
                bosObjectList.clear();
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
        List<BosObjectSummary> futureList = CloudApiUtils.initFutureList(listObjectsRequest.getMaxKeys(), times);
        futureList.addAll(bosObjectList);
        bosObjectList.clear();
        SuitsException exception = null;
        while (futureList.size() < expected && times > 0 && hasNext()) {
            times--;
            try {
                doList();
                count += bosObjectList.size();
                futureList.addAll(bosObjectList);
                bosObjectList.clear();
            } catch (SuitsException e) {
//                bosObjectList = futureList;
//                throw e;
                exception = e;
            }
        }
        bosObjectList = futureList;
        futureList = null;
        if (exception != null) throw exception;
        return hasNext();
    }

    @Override
    public List<BosObjectSummary> currents() {
        return bosObjectList;
    }

    @Override
    public synchronized String currentEndKey() {
        if (hasNext()) return getMarker();
        if (truncateMarker != null && !"".equals(truncateMarker)) return truncateMarker;
        if (last != null) return last.getKey();
        if (bosObjectList.size() > 0) last = bosObjectList.get(bosObjectList.size() - 1);
        if (last != null) return last.getKey();
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
        bosClient.shutdown();
//        listObjectsRequest = null;
        endPrefix = null;
        if (bosObjectList.size() > 0) {
            last = bosObjectList.get(bosObjectList.size() - 1);
            bosObjectList.clear();
        }
    }
}
