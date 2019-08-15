package com.qiniu.process.other;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

public class DownloadFileTest {

    @Test
    public void testProcessLine() {
        try {
            DownloadFile downloadFile = new DownloadFile(null, null, null, "url",
                    null, null,"?v=1", true, null, null, "~/Downloads");
            String result = downloadFile.processLine(new HashMap<String, String>(){{
                put("url", "http://p3l1d5mx4.bkt.clouddn.com/-YVzTgC_I8zlDYIm8eCcPnA76pU=/ltSP7XPbPGviBNjXiZEHX7mpdm6o");
            }});
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}