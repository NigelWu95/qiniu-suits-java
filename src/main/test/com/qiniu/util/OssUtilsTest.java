package com.qiniu.util;

import com.qiniu.config.PropertiesFile;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class OssUtilsTest {

    @Test
    public void getAliOssRegion() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.tencent.properties");
        String secretId = propertiesFile.getValue("ten-id");
        String secretKey = propertiesFile.getValue("ten-secret");
        String bucket = propertiesFile.getValue("bucket");
        String region = OssUtils.getTenCosRegion(secretId, secretKey, bucket);
        System.out.println(region);
    }

    @Test
    public void getTenCosRegion() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.ali.properties");
        String accessKeyId = propertiesFile.getValue("ali-id");
        String accessKeySecret = propertiesFile.getValue("ali-secret");
        String bucket = propertiesFile.getValue("bucket");
        String region = OssUtils.getAliOssRegion(accessKeyId, accessKeySecret, bucket);
        System.out.println(region);
    }
}