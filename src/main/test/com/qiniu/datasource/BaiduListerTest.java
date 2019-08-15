package com.qiniu.datasource;

import com.baidubce.BceServiceException;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.qiniu.config.PropertiesFile;
import com.qiniu.convert.JsonObjectPair;
import com.qiniu.util.ConvertingUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BaiduListerTest {

    @Test
    public void testListing() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKeyId = propertiesFile.getValue("bai-id");
        String secretKey = propertiesFile.getValue("bai-secret");
        String bucket = propertiesFile.getValue("bucket");
        bucket = "nigel-test";
        BosClient bosClient = new BosClient(new BosClientConfiguration().withEndpoint("su.bcebos.com")
                .withCredentials(new DefaultBceCredentials(accessKeyId, secretKey)));
        BaiduLister baiduLister = new BaiduLister(bosClient, bucket, null, null, null, 10000);
//        baiduLister.listForward();
        List<String> fields = new ArrayList<String>(ConvertingUtils.defaultFileFields){{
            remove("mime");
            remove("status");
            remove("md5");
        }};
        List<BosObjectSummary> objectSummaries = baiduLister.currents();
        for (BosObjectSummary objectSummary : objectSummaries) {
            System.out.println(ConvertingUtils.toPair(objectSummary, fields, new JsonObjectPair()));
        }
        baiduLister.close();
    }

    @Test
    public void test() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKeyId = propertiesFile.getValue("bai-id");
        String secretKey = propertiesFile.getValue("bai-secret");
        String bucket = propertiesFile.getValue("bucket");
        bucket = "nigel-test";
        BosClientConfiguration clientConfiguration = new BosClientConfiguration();
        clientConfiguration.setCredentials(new DefaultBceCredentials(accessKeyId, secretKey));
        BosClient bosClient = new BosClient(clientConfiguration);
        // 指定最大返回条数为500
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucket);
        listObjectsRequest.withMaxKeys(50);
        try {
            String region = bosClient.getBucketLocation(bucket).getLocationConstraint();
//            System.out.println(region);
            clientConfiguration.setEndpoint(region + ".bcebos.com");
//            clientConfiguration.setRegion(Region.fromValue("bj"));
            bosClient.shutdown();
//            clientConfiguration.setEndpoint("su.bcebos.com");
            bosClient = new BosClient(clientConfiguration);
            ListObjectsResponse listObjectsResponse = bosClient.listObjects(listObjectsRequest);
            System.out.println("marker: " + listObjectsResponse.getNextMarker());
            for (BosObjectSummary objectSummary : listObjectsResponse.getContents()) {
                System.out.println("ObjectKey:" + objectSummary.getKey());
                System.out.println(" —— " + objectSummary.getOwner());
            }
        } catch (BceServiceException e) {
            System.out.println(e.getErrorCode());
            System.out.println(e.getStatusCode());
            throw e;
        } finally {
            bosClient.shutdown();
        }
    }
}