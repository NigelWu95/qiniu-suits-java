package com.qiniu.datasource;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class TenLister implements Iterator<List<COSObjectSummary>> {

    private COSClient cosClient;
    private String endKeyPrefix;
    private ListObjectsRequest listObjectsRequest;
    private ObjectListing objectListing;
    private List<COSObjectSummary> cosObjectList;
    private int statusCode;
    private String error;

    public TenLister(COSClient cosClient, String bucket, String prefix, String marker, String endKeyPrefix,
                     String delimiter, int max) throws CosClientException {
        this.cosClient = cosClient;
        this.listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        listObjectsRequest.setPrefix(prefix);
        listObjectsRequest.setDelimiter(delimiter);
        listObjectsRequest.setMarker(marker);
        listObjectsRequest.setMaxKeys(max);
        this.endKeyPrefix = endKeyPrefix == null ? "" : endKeyPrefix; // 初始值不使用 null，后续设置时可为空，便于判断是否进行过修改
        this.cosObjectList = getListResult();
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getError() {
        return error;
    }

    public String getPrefix() {
        return listObjectsRequest.getPrefix();
    }

    public void setPrefix(String prefix) {
        listObjectsRequest.setPrefix(prefix);
    }

    public String getMarker() {
        return listObjectsRequest.getMarker();
    }

    public void setMarker(String marker) {
        listObjectsRequest.setMarker(marker);
    }

    public String getEndKeyPrefix() {
        return endKeyPrefix;
    }

    public void setEndKeyPrefix(String endKeyPrefix) {
        this.endKeyPrefix = endKeyPrefix;
    }

    public String getDelimiter() {
        return listObjectsRequest.getDelimiter();
    }

    public int getLimit() {
        return listObjectsRequest.getMaxKeys();
    }

    public List<COSObjectSummary> getCosObjectList() {
        return cosObjectList;
    }

    private List<COSObjectSummary> getListResult() throws CosClientException {
        objectListing = cosClient.listObjects(listObjectsRequest);
        listObjectsRequest.setMarker(objectListing.getNextMarker());
        return objectListing.getObjectSummaries();
    }

    public boolean checkMarkerValid() {
        return objectListing.getNextMarker() != null && !"".equals(objectListing.getNextMarker());
    }

    public boolean checkListValid() {
        return cosObjectList != null && cosObjectList.size() > 0;
    }

    @Override
    public boolean hasNext() {
        return checkMarkerValid() || checkListValid();
    }

    @Override
    public List<COSObjectSummary> next() {
        List<COSObjectSummary> current = cosObjectList == null ? new ArrayList<>() : cosObjectList;
        if (endKeyPrefix != null && !"".equals(endKeyPrefix)) {
            int size = current.size();
            current = current.stream()
                    .filter(objectSummary -> objectSummary.getKey().compareTo(endKeyPrefix) < 0)
                    .collect(Collectors.toList());
            int finalSize = current.size();
            if (finalSize < size) listObjectsRequest.setMarker(null);
        }
        try {
            if (!checkMarkerValid()) cosObjectList = null;
            else {
                do {
                    cosObjectList = getListResult();
                } while (!checkListValid() && checkMarkerValid());
            }
        } catch (CosServiceException e) {
            cosObjectList = null;
            statusCode = e.getStatusCode();
            error = e.getMessage();
        } catch (Exception e) {
            cosObjectList = null;
            statusCode = -1;
            error = e.getMessage();
        }
        return current;
    }

    @Override
    public void remove() {
        this.cosClient.shutdown();
        this.cosObjectList = null;
        this.error = null;
    }
}
