package com.qiniu.datasource;

import com.netease.cloud.ServiceException;
import com.netease.cloud.services.nos.NosClient;
import com.netease.cloud.services.nos.model.ListObjectsRequest;
import com.netease.cloud.services.nos.model.NOSObjectSummary;
import com.netease.cloud.services.nos.model.ObjectListing;
import com.qiniu.common.SuitsException;
import com.qiniu.interfaces.ILister;
import com.qiniu.util.CloudApiUtils;

import java.util.List;

public class NetLister implements ILister<NOSObjectSummary> {

    private NosClient nosClient;
    private ListObjectsRequest listObjectsRequest;
    private String endPrefix;
    private String truncateMarker;
    private List<NOSObjectSummary> nosObjectList;
    private NOSObjectSummary last;
    private long count;

    public NetLister(NosClient nosClient, String bucket, String prefix, String marker, String endPrefix, int max) throws SuitsException {
        this.nosClient = nosClient;
        this.listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        listObjectsRequest.setPrefix(prefix);
        listObjectsRequest.setMarker(marker);
        listObjectsRequest.setMaxKeys(max);
        this.endPrefix = endPrefix;
        doList();
        count += nosObjectList.size();
    }

    @Override
    public String getBucket() {
        return listObjectsRequest.getBucketName();
    }

    public String getPrefix() {
        return listObjectsRequest.getPrefix();
    }

    public void setMarker(String marker) {
        listObjectsRequest.setMarker(marker);
    }

    public String getMarker() {
        return listObjectsRequest.getMarker();
    }

    @Override
    public void setEndPrefix(String endKeyPrefix) {
        this.endPrefix = endKeyPrefix;
        count -= nosObjectList.size();
        checkedListWithEnd();
        count += nosObjectList.size();
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
            if (endPrefix.equals(getPrefix() + CloudStorageContainer.firstPoint) && nosObjectList.size() > 0) {
                int lastIndex = nosObjectList.size() - 1;
                if (endPrefix.equals(nosObjectList.get(lastIndex).getKey())) nosObjectList.remove(lastIndex);
            }
        } else if (endKey.compareTo(endPrefix) > 0) {
            listObjectsRequest.setMarker(null);
            int size = nosObjectList.size();
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            int i = 0;
            for (; i < size; i++) {
                if (nosObjectList.get(i).getKey().compareTo(endPrefix) > 0) {
//                    nosObjectList.remove(i);
                    break;
                }
            }
            // 优化 gc，不用的元素全部清除
            nosObjectList.subList(i, size).clear();
        }

    }

    private void doList() throws SuitsException {
        try {
            ObjectListing objectListing = nosClient.listObjects(listObjectsRequest);
            listObjectsRequest.setMarker(objectListing.getNextMarker());
            nosObjectList = objectListing.getObjectSummaries();
            checkedListWithEnd();
        } catch (ServiceException e) {
            throw new SuitsException(e, CloudApiUtils.NetStatusCode(e.getErrorCode(), -1));
        } catch (NullPointerException e) {
            throw new SuitsException(e, 400000, "lister maybe already closed");
        } catch (Exception e) {
            throw new SuitsException(e, -1, "listing failed");
        }
    }

    @Override
    public synchronized void listForward() throws SuitsException {
        if (hasNext()) {
            nosObjectList.clear();
            doList();
            count += nosObjectList.size();
        } else {
            if (nosObjectList.size() > 0) {
                last = nosObjectList.get(nosObjectList.size() - 1);
                nosObjectList.clear();
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
        List<NOSObjectSummary> futureList = CloudApiUtils.initFutureList(listObjectsRequest.getMaxKeys(), times);
        futureList.addAll(nosObjectList);
        nosObjectList.clear();
        SuitsException exception = null;
        while (futureList.size() < expected && times > 0 && hasNext()) {
            times--;
            try {
                doList();
                count += nosObjectList.size();
                futureList.addAll(nosObjectList);
                nosObjectList.clear();
            } catch (SuitsException e) {
//                nosObjectList = futureList;
//                throw e;
                exception = e;
            }
        }
        nosObjectList = futureList;
        futureList = null;
        if (exception != null) throw exception;
        return hasNext();
    }

    @Override
    public List<NOSObjectSummary> currents() {
        return nosObjectList;
    }

    @Override
    public synchronized String currentEndKey() {
        if (hasNext()) return getMarker();
        if (truncateMarker != null && !"".equals(truncateMarker)) return truncateMarker;
        if (last != null) return last.getKey();
        if (nosObjectList.size() > 0) last = nosObjectList.get(nosObjectList.size() - 1);
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
        nosClient.shutdown();
//        listObjectsRequest = null;
        endPrefix = null;
        if (nosObjectList.size() > 0) {
            last = nosObjectList.get(nosObjectList.size() - 1);
            nosObjectList.clear();
        }
    }
}
