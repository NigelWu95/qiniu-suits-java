package com.qiniu.process.other;

import org.junit.Test;

import java.io.IOException;

public class HttpDownloaderTest {

    @Test
    public void testDownload() {
        HttpDownloader downloader = new HttpDownloader();
        try {
            downloader.download("http://p3l1d5mx4.bkt.clouddn.com/-YVzTgC_I8zlDYIm8eCcPnA76pU=/ltSP7XPbPGviBNjXiZEHX7mpdm6o",
                    "/Users/wubingheng/Downloads/ltSP7XPbPGviBNjXiZEHX7mpdm6o", null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}