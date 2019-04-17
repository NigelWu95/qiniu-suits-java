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
import java.util.stream.Collectors;

public class AliLister implements ILister<OSSObjectSummary> {

    private OSSClient ossClient;
    private String endPrefix;
    private ListObjectsRequest listObjectsRequest;
    private ObjectListing objectListing;
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

    @Override
    public void setEndPrefix(String endKeyPrefix) {
        this.endPrefix = endKeyPrefix;
        if (endPrefix != null && !"".equals(endPrefix)) {
            int size = ossObjectList.size();
            ossObjectList = ossObjectList.stream()
                    .filter(objectSummary -> objectSummary.getKey().compareTo(endPrefix) < 0)
                    .collect(Collectors.toList());
            if (ossObjectList.size() < size) listObjectsRequest.setMarker(null);
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

    private List<OSSObjectSummary> getListResult() throws OSSException, ClientException {
        objectListing = ossClient.listObjects(listObjectsRequest);
        listObjectsRequest.setMarker(objectListing.getNextMarker());
        return objectListing.getObjectSummaries();
    }

    @Override
    public void listForward() throws SuitsException {
        try {
            List<OSSObjectSummary> current;
            do {
                current = getListResult();
            } while (current.size() == 0 && hasNext());

            if (endPrefix != null && !"".equals(endPrefix)) {
                ossObjectList = current.stream()
                        .filter(objectSummary -> objectSummary.getKey().compareTo(endPrefix) < 0)
                        .collect(Collectors.toList());
                if (ossObjectList.size() < current.size()) listObjectsRequest.setMarker(null);
            } else {
                ossObjectList = current;
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
        return objectListing.getNextMarker() != null && !"".equals(objectListing.getNextMarker());
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
