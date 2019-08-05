package com.qiniu.process.qai;

import com.qiniu.config.PropertiesFile;
import com.qiniu.storage.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;

public class QueryCensorResultTest {

    @Test
    public void singleResult() throws IOException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        QueryCensorResult queryCensorResult = new QueryCensorResult(accessKey, secretKey, new Configuration(), "jodId");
        String result = queryCensorResult.singleResult(new HashMap<String, String>(){{
            put("jodId", "5d480ea3244bbb000818d0f4");
        }});
        System.out.println(result);
    }
}