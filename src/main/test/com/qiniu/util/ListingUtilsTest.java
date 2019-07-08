package com.qiniu.util;

import com.google.gson.JsonObject;
import com.qiniu.config.PropertiesFile;
import com.qiniu.sdk.FileItem;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ListingUtilsTest {

    @Test
    public void testGetTenCosRegion() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.tencent.properties");
        String secretId = propertiesFile.getValue("ten-id");
        String secretKey = propertiesFile.getValue("ten-secret");
        String bucket = propertiesFile.getValue("bucket");
        String region = ListingUtils.getTenCosRegion(secretId, secretKey, bucket);
        System.out.println(bucket + "\t" + region);
    }

    @Test
    public void testGetS3Region() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.s3.properties");
        String accessId = propertiesFile.getValue("s3-id");
        String secretKey = propertiesFile.getValue("s3-secret");
        String bucket = propertiesFile.getValue("bucket");
        System.out.println(bucket + "\t" + ListingUtils.getS3Region(accessId, secretKey, bucket + "1"));
        System.out.println(bucket + "\t" + ListingUtils.getS3Region(accessId, secretKey, bucket + "2"));
        System.out.println(bucket + "\t" + ListingUtils.getS3Region(accessId, secretKey, bucket + "3"));
    }

    @Test
    public void testGetAliOssRegion() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.ali.properties");
        String accessKeyId = propertiesFile.getValue("ali-id");
        String accessKeySecret = propertiesFile.getValue("ali-secret");
        String bucket = propertiesFile.getValue("bucket");
        System.out.println(bucket + "\t" + ListingUtils.getAliOssRegion(accessKeyId, accessKeySecret, bucket));
        System.out.println(bucket + "\t" + ListingUtils.getAliOssRegion(accessKeyId, accessKeySecret, bucket + "2"));
        System.out.println(bucket + "\t" + ListingUtils.getAliOssRegion(accessKeyId, accessKeySecret, bucket + "3"));
    }

    @Test
    public void testGetUpYunMarker() {
        String bucket = "squirrel";
        String name1 = "wordSplit/xml/20161220/FF8080815919A151015919D7DC8F0036";
        FileItem fileItem1 = new FileItem();
        fileItem1.key = name1;
        fileItem1.attribute = "folder";
        System.out.println(ListingUtils.getUpYunMarker(bucket, fileItem1));
        String name2 = "wordSplit/xml/20161220/FF8080815919A15101591AFE37C603F7/4028965B591534B501591BBEC0E8049A.txt";
        FileItem fileItem2 = new FileItem();
        fileItem2.key = name2;
        System.out.println(ListingUtils.getUpYunMarker(bucket, fileItem2));
    }

    @Test
    public void testDecodeUpYunMarker() {
        String marker1 = "c3F1aXJyZWwvfndvcmRTcGxpdC9+eG1sL34yMDE2MTIyMC9AfkZGODA4MDgxNTkxOUExNTEwMTU5MTlEN0RDOEYwMDM2";
        String marker2 = "c3F1aXJyZWwvfndvcmRTcGxpdC9+eG1sL34yMDE2MTIyMC9+RkY4MDgwODE1OTE5QTE1MTAxNTkxQUZFMzdDNjAzRjcvQCM0MDI4OTY1QjU5MTUzNEI1MDE1OTFCQkVDMEU4MDQ5QS50eHQ=";
        System.out.println(ListingUtils.decodeUpYunMarker(marker1));
        System.out.println(ListingUtils.decodeUpYunMarker(marker2));
    }

    @Test
    public void testPrefixConfig() {
        ListingUtils.recordPrefixConfig("a", null);
        JsonObject json = new JsonObject();
        json.addProperty("start", "a");
        ListingUtils.recordPrefixConfig("a", json);
    }

    @Test
    public void testWriteContinuedPrefixConfig() throws IOException {
        File file = new File("./");
        System.out.println(file);
        System.out.println(file.exists());
        System.out.println(file.isDirectory());
        System.out.println(file.getCanonicalPath());
        System.out.println(file.getAbsolutePath());
        file = new File(file.getAbsolutePath());
        System.out.println(file.getParent());
        ListingUtils.writeContinuedPrefixConfig("./", "test");
    }
}