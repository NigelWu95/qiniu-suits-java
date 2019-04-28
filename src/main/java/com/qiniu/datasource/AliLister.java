package com.qiniu.datasource;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.qiniu.Constants.OssStatus;
import com.qiniu.common.SuitsException;

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
        this.endPrefix = endPrefix == null ? "" : endPrefix; // 初始值不使用 null，后续设置时可为空，便于判断是否进行过修改
        listForward();
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

    private void checkedListWithEnd() {
        int size = ossObjectList.size();
        if (size > 0) {
            // SDK 中返回的是 ArrayList，使用 remove 操作性能一般较差，同时也为了避免 Collectors.toList() 的频繁 new 操作，根据返
            // 回的 list 为文件名有序的特性，直接从 end 的位置进行截断
            for (int i = 0; i < ossObjectList.size(); i++) {
                if (ossObjectList.get(i).getKey().compareTo(endPrefix) >= 0) {
                    ossObjectList = ossObjectList.subList(0, i);
                    break;
                }
            }
            if (ossObjectList.size() < size) listObjectsRequest.setMarker(null);
        } else if (currentLastKey() != null && currentLastKey().compareTo(endPrefix) >= 0) {
            listObjectsRequest.setMarker(null);
        }
    }

    @Override
    public void setEndPrefix(String endKeyPrefix) {
        this.endPrefix = endKeyPrefix;
        if (endPrefix != null && !"".equals(endPrefix)) {
            checkedListWithEnd();
        }
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
    public boolean canStraight() {
        return straight || !hasNext() || (endPrefix != null && !"".equals(endPrefix));
    }

    private List<OSSObjectSummary> getListResult() throws OSSException, ClientException {
        ObjectListing objectListing = ossClient.listObjects(listObjectsRequest);
        listObjectsRequest.setMarker(objectListing.getNextMarker());
        return objectListing.getObjectSummaries();
    }

    @Override
    public void listForward() throws SuitsException {
        try {
            ossObjectList = getListResult();
            if (endPrefix != null && !"".equals(endPrefix)) {
                checkedListWithEnd();
            }
        } catch (ClientException e) {
            int code = OssStatus.aliMap.getOrDefault(e.getErrorCode(), -1);
            throw new SuitsException(code, e.getMessage());
        } catch (ServiceException e) {
            int code = OssStatus.aliMap.getOrDefault(e.getErrorCode(), -1);
            throw new SuitsException(code, e.getMessage());
        } catch (Exception e) {
            throw new SuitsException(-1, "failed, " + e.getMessage());
        }
    }

    @Override
    public boolean hasNext() {
        return listObjectsRequest.getMarker() != null && !"".equals(listObjectsRequest.getMarker());
    }

    @Override
    public boolean hasFutureNext() throws SuitsException {
        int times = 50000 / listObjectsRequest.getMaxKeys();
        times = times > 10 ? 10 : times;
        List<OSSObjectSummary> futureList = ossObjectList;
        while (hasNext() && times > 0 && futureList.size() < 10001) {
            times--;
            try {
                ossObjectList = getListResult();
                if (endPrefix != null && !"".equals(endPrefix)) {
                    checkedListWithEnd();
                }
            } catch (ClientException e) {
                int code = OssStatus.aliMap.getOrDefault(e.getErrorCode(), -1);
                throw new SuitsException(code, e.getMessage());
            } catch (ServiceException e) {
                int code = OssStatus.aliMap.getOrDefault(e.getErrorCode(), -1);
                throw new SuitsException(code, e.getMessage());
            }
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
    public OSSObjectSummary currentFirst() {
        return ossObjectList.size() > 0 ? ossObjectList.get(0) : null;
    }

    @Override
    public String currentFirstKey() {
        OSSObjectSummary first = currentFirst();
        return first != null ? first.getKey() : null;
    }

    @Override
    public OSSObjectSummary currentLast() {
        OSSObjectSummary last = ossObjectList.size() > 0 ? ossObjectList.get(ossObjectList.size() - 1) : null;
        if (last == null) {
            last = new OSSObjectSummary();
            last.setKey(getMarker());
        }
        return last;
    }

    @Override
    public String currentLastKey() {
        OSSObjectSummary last = currentLast();
        return last != null ? last.getKey() : null;
    }

    @Override
    public void updateMarkerBy(OSSObjectSummary object) {
        listObjectsRequest.setMarker(object.getKey());
    }

    @Override
    public void close() {
        this.ossClient.shutdown();
        this.ossObjectList = null;
    }
}
