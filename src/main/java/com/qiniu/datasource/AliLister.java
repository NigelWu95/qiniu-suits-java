package com.qiniu.datasource;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.qiniu.common.SuitsException;
import com.qiniu.util.OssUtils;

import java.util.List;

public class AliLister implements ILister<OSSObjectSummary> {

    private OSSClient ossClient;
    private String endPrefix;
    private ListObjectsRequest listObjectsRequest;
    private boolean straight;
    private List<OSSObjectSummary> ossObjectList;

    public AliLister(OSSClient ossClient, String bucket, String prefix, String marker, String endPrefix,
                     String delimiter, int max) throws SuitsException {
        this.ossClient = ossClient;
        this.listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        listObjectsRequest.setPrefix(prefix);
        listObjectsRequest.setDelimiter(delimiter);
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
    public void setEndPrefix(String endKeyPrefix) {
        this.endPrefix = endKeyPrefix;
        checkedListWithEnd();
    }

    @Override
    public String getEndPrefix() {
        return endPrefix;
    }

    @Override
    public void setDelimiter(String delimiter) {
        listObjectsRequest.setDelimiter(delimiter);
    }

    @Override
    public String getDelimiter() {
        return listObjectsRequest.getDelimiter();
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
            int size = ossObjectList.size();
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            for (int i = 0; i < size; i++) {
                if (ossObjectList.get(i).getKey().compareTo(endPrefix) > 0) {
                    ossObjectList = ossObjectList.subList(0, i);
                    listObjectsRequest.setMarker(null);
                    return;
                }
            }
        }

    }

    private void doList() throws SuitsException {
        try {
            ObjectListing objectListing = ossClient.listObjects(listObjectsRequest);
            listObjectsRequest.setMarker(objectListing.getNextMarker());
            ossObjectList = objectListing.getObjectSummaries();
            checkedListWithEnd();
        } catch (ClientException e) {
            int code = OssUtils.AliStatusCode(e.getErrorCode(), -1);
            throw new SuitsException(code, e.getMessage());
        } catch (ServiceException e) {
            int code = OssUtils.AliStatusCode(e.getErrorCode(), -1);
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
            ossObjectList.clear();
        }
    }

    @Override
    public boolean hasNext() {
        return listObjectsRequest.getMarker() != null && !"".equals(listObjectsRequest.getMarker());
    }

    @Override
    public boolean hasFutureNext() throws SuitsException {
        int times = 50000 / (ossObjectList.size() + 1);
        times = times > 10 ? 10 : times;
        List<OSSObjectSummary> futureList = ossObjectList;
        while (hasNext() && times > 0 && futureList.size() < 10001) {
            times--;
            doList();
            futureList.addAll(ossObjectList);
        }
        ossObjectList = futureList;
        return hasNext();
    }

    @Override
    public List<OSSObjectSummary> currents() {
        return ossObjectList;
    }

    @Override
    public OSSObjectSummary currentLast() {
        return ossObjectList.size() > 0 ? ossObjectList.get(ossObjectList.size() - 1) : null;
//        if (last == null && hasNext()) {
//            last = new OSSObjectSummary();
//            last.setKey(getMarker());
//        }
//        return last;
    }

    @Override
    public String currentEndKey() {
        if (hasNext()) return getMarker();
        OSSObjectSummary last = currentLast();
        return last != null ? last.getKey() : null;
    }

    @Override
    public void updateMarkerBy(OSSObjectSummary object) {
        if (object != null) listObjectsRequest.setMarker(OssUtils.getAliOssMarker(object.getKey()));
    }

    @Override
    public void close() {
        this.ossClient.shutdown();
        this.ossObjectList = null;
    }
}
