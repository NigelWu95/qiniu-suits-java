package com.qiniu.sdk;

import com.qiniu.config.PropertiesFile;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class UpYunClientTest {

    @Test
    public void testGetFileInfo() throws Exception {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String username = propertiesFile.getValue("up-id");
        String password = propertiesFile.getValue("up-secret");
        String bucket = propertiesFile.getValue("bucket");
        UpYunConfig configuration = new UpYunConfig();
        UpYunClient upYunClient = new UpYunClient(configuration, username, password);
        FileItem fileItem = upYunClient.getFileInfo(bucket, "listbucket.go");
        System.out.println(fileItem.size);
    }
}