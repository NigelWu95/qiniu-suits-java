package com.qiniu.datasource;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;

import java.util.List;
import java.util.stream.Collectors;

public class TenLister implements ILister<COSObjectSummary> {

    private COSClient cosClient;
    private String endPrefix;
    private ListObjectsRequest listObjectsRequest;
    private boolean straight;
    private List<COSObjectSummary> cosObjectList;

    public TenLister(COSClient cosClient, String bucket, String prefix, String marker, String endPrefix,
                     String delimiter, int max) throws SuitsException {
        this.cosClient = cosClient;
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
            int size = cosObjectList.size();
            cosObjectList = cosObjectList.stream()
                    .filter(objectSummary -> objectSummary.getKey().compareTo(endPrefix) < 0)
                    .collect(Collectors.toList());
            if (cosObjectList.size() < size) listObjectsRequest.setMarker(null);
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
        return straight;
    }

    private List<COSObjectSummary> getListResult() throws CosClientException {
        ObjectListing objectListing = cosClient.listObjects(listObjectsRequest);
        listObjectsRequest.setMarker(objectListing.getNextMarker());
        return objectListing.getObjectSummaries();
    }

    @Override
    public void listForward() throws SuitsException {
        try {
            List<COSObjectSummary> current;
            do {
                current = getListResult();
            } while (current.size() == 0 && hasNext());

            if (endPrefix != null && !"".equals(endPrefix)) {
                cosObjectList = current.stream()
                        .filter(objectSummary -> objectSummary.getKey().compareTo(endPrefix) < 0)
                        .collect(Collectors.toList());
                if (cosObjectList.size() < current.size()) listObjectsRequest.setMarker(null);
            } else {
                cosObjectList = current;
            }
        } catch (CosServiceException e) {
            throw new SuitsException(e.getStatusCode(), e.getMessage());
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
        return hasNext();
    }

    @Override
    public List<COSObjectSummary> currents() {
        return cosObjectList;
    }

    @Override
    public COSObjectSummary currentFirst() {
        return cosObjectList.size() > 0 ? cosObjectList.get(0) : null;
    }

    @Override
    public String currentFirstKey() {
        COSObjectSummary first = currentFirst();
        return first != null ? first.getKey() : null;
    }

    @Override
    public COSObjectSummary currentLast() {
        COSObjectSummary last = cosObjectList.size() > 0 ? cosObjectList.get(cosObjectList.size() - 1) : null;
        if (last == null) {
            last = new COSObjectSummary();
            last.setKey(getMarker());
        }
        return last;
    }

    @Override
    public String currentLastKey() {
        COSObjectSummary last = currentLast();
        return last != null ? last.getKey() : null;
    }

    @Override
    public void updateMarkerBy(COSObjectSummary object) {
        listObjectsRequest.setMarker(object.getKey());
    }

    @Override
    public void close() {
        this.cosClient.shutdown();
        this.cosObjectList = null;
    }
}
