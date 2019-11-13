package com.qiniu.process.other;

import com.qiniu.util.StringMap;
import org.junit.Test;

import java.io.IOException;

public class HttpDownloaderTest {

    @Test
    public void testDownload() {
        HttpDownloader downloader = new HttpDownloader();
        try {
            StringMap headers = new StringMap().put("Range", "bytes=0-");
//            downloader.download("http://xxx.com/-YVzTgC_I8zlDYIm8eCcPnA76pU=/ltSP7XPbPGviBNjXiZEHX7mpdm6o",
//                    "/Users/wubingheng/Downloads/ltSP7XPbPGviBNjXiZEHX7mpdm6o", null);
            downloader.download("http://xxx.com/-YVzTgC_I8zlDYIm8eCcPnA76pU=/ltSP7XPbPGviBNjXiZEHX7mpdm6o",
                    headers);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}