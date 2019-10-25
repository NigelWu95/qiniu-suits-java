package com.qiniu.datasource;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.qiniu.common.SuitsException;
import com.qiniu.interfaces.ILister;
import com.qiniu.util.CloudApiUtils;

import java.util.List;

public class AliLister implements ILister<OSSObjectSummary> {

    private OSSClient ossClient;
    private ListObjectsRequest listObjectsRequest;
    private String endPrefix;
    private String truncateMarker;
    private List<OSSObjectSummary> ossObjectList;
    private OSSObjectSummary last;
    private long count;

    public AliLister(OSSClient ossClient, String bucket, String prefix, String marker, String endPrefix, int max) throws SuitsException {
        this.ossClient = ossClient;
        this.listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        listObjectsRequest.setPrefix(prefix);
        listObjectsRequest.setMarker("".equals(marker) ? null : marker);
        listObjectsRequest.setMaxKeys(max);
        this.endPrefix = endPrefix;
        doList();
        count += ossObjectList.size();
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
        count -= ossObjectList.size();
        checkedListWithEnd();
        count += ossObjectList.size();
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
            if (endPrefix.equals(getPrefix() + CloudStorageContainer.firstPoint) && ossObjectList.size() > 0) {
                int lastIndex = ossObjectList.size() - 1;
                if (endPrefix.equals(ossObjectList.get(lastIndex).getKey())) ossObjectList.remove(lastIndex);
            }
        } else if (endKey.compareTo(endPrefix) > 0) {
            listObjectsRequest.setMarker(null);
            int size = ossObjectList.size();
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            int i = 0;
            for (; i < size; i++) {
                if (ossObjectList.get(i).getKey().compareTo(endPrefix) > 0) {
//                    ossObjectList.remove(i);
                    break;
                }
            }
            // 优化 gc，不用的元素全部清除
            ossObjectList.subList(i, size).clear();
        }
    }

    private void doList() throws SuitsException {
        try {
            ObjectListing objectListing = ossClient.listObjects(listObjectsRequest);
            listObjectsRequest.setMarker(objectListing.getNextMarker());
            ossObjectList = objectListing.getObjectSummaries();
            checkedListWithEnd();
        } catch (ClientException e) {
            throw new SuitsException(e, CloudApiUtils.AliStatusCode(e.getErrorCode(), -1));
        } catch (ServiceException e) {
            throw new SuitsException(e, CloudApiUtils.AliStatusCode(e.getErrorCode(), -1));
        } catch (NullPointerException e) {
            throw new SuitsException(e, 400000, "lister maybe already closed");
        } catch (Exception e) {
            throw new SuitsException(e, -1, "listing failed");
        }
    }

    @Override
    public synchronized void listForward() throws SuitsException {
        if (hasNext()) {
            ossObjectList.clear();
            doList();
            count += ossObjectList.size();
        } else {
            if (ossObjectList.size() > 0) {
                last = ossObjectList.get(ossObjectList.size() - 1);
                ossObjectList.clear();
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
        List<OSSObjectSummary> futureList = CloudApiUtils.initFutureList(listObjectsRequest.getMaxKeys(), times);
        futureList.addAll(ossObjectList);
        ossObjectList.clear();
        SuitsException exception = null;
        while (futureList.size() < expected && times > 0 && hasNext()) {
            times--;
            try {
                doList();
                count += ossObjectList.size();
                futureList.addAll(ossObjectList);
                ossObjectList.clear();
            } catch (SuitsException e) {
//                ossObjectList = futureList;
//                throw e;
                exception = e;
            }
        }
        ossObjectList = futureList;
        futureList = null;
        if (exception != null) throw exception;
        return hasNext();
    }

    @Override
    public List<OSSObjectSummary> currents() {
        return ossObjectList;
    }

    @Override
    public synchronized String currentEndKey() {
        if (hasNext()) return getMarker();
        if (truncateMarker != null && !"".equals(truncateMarker)) return truncateMarker;
        if (last != null) return last.getKey();
        if (ossObjectList.size() > 0) last = ossObjectList.get(ossObjectList.size() - 1);
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
        ossClient.shutdown();
//        listObjectsRequest = null;
        endPrefix = null;
        if (ossObjectList.size() > 0) {
            last = ossObjectList.get(ossObjectList.size() - 1);
            ossObjectList.clear();
        }
    }
}
