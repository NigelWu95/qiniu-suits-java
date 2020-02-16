package com.qiniu.process.qiniu;

import com.qiniu.config.PropertiesFile;
import com.qiniu.storage.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class CdnUrlQueryTest {

    @Test
    public void testProcess() throws IOException {

        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        CdnUrlQuery cdnUrlQuery = new CdnUrlQuery(accessKey, secretKey, new Configuration(), null, null, "url", true,
                "/Users/wubingheng/Downloads/refresh");
        List<Map<String, String>> list = new ArrayList<>();
        list.add(new HashMap<String, String>(){{
            put("url", "http://xxx.com/1.mp3");
        }});
        list.add(new HashMap<String, String>(){{
            put("url", "http://xxx.com/181224-观点峰会-1.mp3");
        }});
        list.add(new HashMap<String, String>(){{
            put("url", "http://xxx.com/VoiceLibrary_MTQ2Njg2Mw");
        }});
        cdnUrlQuery.processLine(list);
        cdnUrlQuery.closeResource();
    }
}