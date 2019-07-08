package com.qiniu.datasource;

import com.netease.cloud.ServiceException;
import com.netease.cloud.services.nos.NosClient;
import com.netease.cloud.services.nos.model.ListObjectsRequest;
import com.netease.cloud.services.nos.model.NOSObjectSummary;
import com.netease.cloud.services.nos.model.ObjectListing;
import com.qiniu.common.SuitsException;
import com.qiniu.util.ListingUtils;

import java.util.List;

public class NetLister implements ILister<NOSObjectSummary> {

    private NosClient nosClient;
    private String endPrefix;
    private ListObjectsRequest listObjectsRequest;
    private boolean straight;
    private List<NOSObjectSummary> nosObjectList;

    public NetLister(NosClient nosClient, String bucket, String prefix, String marker, String endPrefix, int max) throws SuitsException {
        this.nosClient = nosClient;
        this.listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        listObjectsRequest.setPrefix(prefix);
        listObjectsRequest.setMarker(marker);
        listObjectsRequest.setMaxKeys(max);
        this.endPrefix = endPrefix;
        doList();
    }

    public void setPrefix(String prefix) {
        listObjectsRequest.setPrefix(prefix);
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
    public synchronized void setEndPrefix(String endKeyPrefix) {
        this.endPrefix = endKeyPrefix;
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

    @Override
    public void setStraight(boolean straight) {
        this.straight = straight;
    }

    @Override
    public boolean canStraight() {
        return straight || !hasNext() || (endPrefix != null && !"".equals(endPrefix));
    }

    private void checkedListWithEnd() {
        String endKey = currentEndKey();
        if (endPrefix == null || "".equals(endPrefix) || endKey == null) return;
        if (endKey.compareTo(endPrefix) == 0) {
            listObjectsRequest.setMarker(null);
            if (endPrefix.equals(getPrefix() + CloudStorageContainer.startPoint)) {
                NOSObjectSummary last = currentLast();
                if (last != null && endPrefix.equals(last.getKey()))
                    nosObjectList.remove(last);
            }
        } else if (endKey.compareTo(endPrefix) > 0) {
            listObjectsRequest.setMarker(null);
            int size = nosObjectList.size();
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            for (int i = 0; i < size; i++) {
                if (nosObjectList.get(i).getKey().compareTo(endPrefix) > 0) {
                    nosObjectList = nosObjectList.subList(0, i);
                    return;
                }
            }
        }

    }

    private synchronized void doList() throws SuitsException {
        try {
            ObjectListing objectListing = nosClient.listObjects(listObjectsRequest);
            listObjectsRequest.setMarker(objectListing.getNextMarker());
            nosObjectList = objectListing.getObjectSummaries();
            checkedListWithEnd();
        } catch (ServiceException e) {
            int code = ListingUtils.NetStatusCode(e.getErrorCode(), -1);
            throw new SuitsException(code, e.getMessage());
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
            nosObjectList.clear();
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
        int times = 100000 / (nosObjectList.size() + 1) + 1;
        times = times > 10 ? 10 : times;
        List<NOSObjectSummary> futureList = nosObjectList;
        while (hasNext() && times > 0 && futureList.size() < expected) {
            times--;
            doList();
            futureList.addAll(nosObjectList);
        }
        nosObjectList = futureList;
        return hasNext();
    }

    @Override
    public List<NOSObjectSummary> currents() {
        return nosObjectList;
    }

    @Override
    public NOSObjectSummary currentLast() {
        return nosObjectList.size() > 0 ? nosObjectList.get(nosObjectList.size() - 1) : null;
    }

    @Override
    public String currentStartKey() {
        return nosObjectList.size() > 0 ? nosObjectList.get(0).getKey() : null;
    }

    @Override
    public String currentEndKey() {
        if (hasNext()) return getMarker();
        NOSObjectSummary last = currentLast();
        return last != null ? last.getKey() : null;
    }

    @Override
    public void updateMarkerBy(NOSObjectSummary object) {
        if (object != null) listObjectsRequest.setMarker(ListingUtils.getAliOssMarker(object.getKey()));
    }

    @Override
    public void close() {
        nosClient.shutdown();
        listObjectsRequest = null;
        endPrefix = null;
        nosObjectList = null;
    }
}
