package com.qiniu.util;

import com.qiniu.config.PropertiesFile;
import com.qiniu.sdk.FileItem;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class OssUtilsTest {

    @Test
    public void testGetAliOssRegion() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.tencent.properties");
        String secretId = propertiesFile.getValue("ten-id");
        String secretKey = propertiesFile.getValue("ten-secret");
        String bucket = propertiesFile.getValue("bucket");
        String region = OssUtils.getTenCosRegion(secretId, secretKey, bucket);
        System.out.println(region);
    }

    @Test
    public void testGetTenCosRegion() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.ali.properties");
        String accessKeyId = propertiesFile.getValue("ali-id");
        String accessKeySecret = propertiesFile.getValue("ali-secret");
        String bucket = propertiesFile.getValue("bucket");
        String region = OssUtils.getAliOssRegion(accessKeyId, accessKeySecret, bucket);
        System.out.println(region);
    }

    @Test
    public void testGetUpYunMarker() {
        String bucket = "squirrel";
        String name1 = "wordSplit/xml/20161220/FF8080815919A151015919D7DC8F0036";
        FileItem fileItem1 = new FileItem();
        fileItem1.key = name1;
        fileItem1.attribute = "folder";
        System.out.println(OssUtils.getUpYunMarker(bucket, fileItem1));
        String name2 = "wordSplit/xml/20161220/FF8080815919A15101591AFE37C603F7/4028965B591534B501591BBEC0E8049A.txt";
        FileItem fileItem2 = new FileItem();
        fileItem2.key = name2;
        System.out.println(OssUtils.getUpYunMarker(bucket, fileItem2));
    }

    @Test
    public void testDecodeUpYunMarker() {
        String marker1 = "c3F1aXJyZWwvfndvcmRTcGxpdC9+eG1sL34yMDE2MTIyMC9AfkZGODA4MDgxNTkxOUExNTEwMTU5MTlEN0RDOEYwMDM2";
        String marker2 = "c3F1aXJyZWwvfndvcmRTcGxpdC9+eG1sL34yMDE2MTIyMC9+RkY4MDgwODE1OTE5QTE1MTAxNTkxQUZFMzdDNjAzRjcvQCM0MDI4OTY1QjU5MTUzNEI1MDE1OTFCQkVDMEU4MDQ5QS50eHQ=";
        System.out.println(OssUtils.decodeUpYunMarker(marker1));
        System.out.println(OssUtils.decodeUpYunMarker(marker2));
    }
}