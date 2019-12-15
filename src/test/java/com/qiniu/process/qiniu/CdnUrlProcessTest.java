package com.qiniu.process.qiniu;

import com.qiniu.config.PropertiesFile;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CdnUrlProcessTest {

    @Test
    public void testSingleRefresh() throws IOException {

        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        CdnUrlProcess cdnUrlProcess = new CdnUrlProcess(accessKey, secretKey, null, null, "url", false, false);
        String result = cdnUrlProcess.singleResult(new HashMap<String, String>(){{
            put("url", "http://xxx.cn/upload/24790f63-0936-44c4-8695-a6d6b1dd8d91.jpg");
        }});
        System.out.println(result);
    }

    @Test
    public void testBatchRefresh() throws IOException {

        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        CdnUrlProcess cdnUrlProcess = new CdnUrlProcess(accessKey, secretKey, null, null, "url", true, false,
                "/Users/wubingheng/Downloads/refresh");
        List<Map<String, String>> list = new ArrayList<>();
//        list.add(new HashMap<String, String>(){{
//            put("url", "http://qsuits.nigel.net.cn/choose_install_java_message.jpg");
//        }});
//        list.add(new HashMap<String, String>(){{
//            put("url", "http://qsuits.nigel.net.cn/choose_install_java_message.jpg");
//        }});
//        list.add(new HashMap<String, String>(){{
//            put("url", "http://test.nigel.net.cn/choose_install_java_message.jpg");
//        }});
//        list.add(new HashMap<String, String>(){{
//            put("url", "http://qsuits.nigel.net.cn/choose_install_java_message.jpg");
//        }});
        list.add(new HashMap<String, String>(){{
            put("url", "http://qsuits.nigel.net.cn/choose_install_java_message.jpg");
        }});
//        list.add(new HashMap<String, String>(){{
//            put("url", "http://qsuits.nigel.net.cn/choose_install_java_message.jpg");
//        }});
        list.add(new HashMap<String, String>(){{
            put("url", "http://xxx.cn/upload/24790f63-0936-44c4-8695-a6d6b1dd8d91.jpg");
        }});
        cdnUrlProcess.processLine(list);
        cdnUrlProcess.closeResource();
    }

    @Test
    public void testBatchPrefetch() throws IOException {

        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        CdnUrlProcess cdnUrlProcess = new CdnUrlProcess(accessKey, secretKey, null, null, "url", false, true,
                "/Users/wubingheng/Downloads/refresh");
        List<Map<String, String>> list = new ArrayList<>();
        list.add(new HashMap<String, String>(){{
            put("url", "http://qsuits.nigel.net.cn/choose_install_java_message.jpg");
        }});
//        list.add(new HashMap<String, String>(){{
//            put("url", "http://qsuits.nigel.net.cn/choose_install_java_message.jpg");
//        }});
//        list.add(new HashMap<String, String>(){{
//            put("url", "http://xxx.cn/upload/24790f63-0936-44c4-8695-a6d6b1dd8d91.jpg");
//        }});
        cdnUrlProcess.processLine(list);
        cdnUrlProcess.closeResource();
    }

    @Test
    public void testClone() throws IOException, CloneNotSupportedException {
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        CdnUrlProcess cdnUrlProcess = new CdnUrlProcess(accessKey, secretKey, null, null, "url", false, true,
                "/Users/wubingheng/Downloads/refresh");
        System.out.println(cdnUrlProcess);
        CdnUrlProcess cdnUrlProcess1 = cdnUrlProcess.clone();
        System.out.println(cdnUrlProcess1);
    }
}