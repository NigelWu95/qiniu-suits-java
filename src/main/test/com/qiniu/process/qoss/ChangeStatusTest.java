package com.qiniu.process.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.config.PropertiesFile;
import com.qiniu.storage.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ChangeStatusTest {

    private ChangeStatus changeStatus;

    @Before
    public void init() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        String bucket = propertiesFile.getValue("bucket");
        changeStatus = new ChangeStatus(accessKey, secretKey, new Configuration(), bucket, 1, "../xhs.txt");
    }

    @Test
    public void singleResult() {
        Map<String, String> map = new HashMap<String, String>(){{
            put("key", "f3aae9d7-3dcd-7bb0-951a-30bba0b70dc3.html");
        }};
        try {
            String result = changeStatus.singleResult(map);
            System.out.println(result);
        } catch (QiniuException e) {
            e.printStackTrace();
        }
    }
}