package com.qiniu.service.media;

import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class M3U8ManagerTest {

    @Test
    public void getVideoTSListByUrl() throws IOException {

        M3U8Manager manager = new M3U8Manager();
        List<VideoTS> videoTSList = manager.getVideoTSListByUrl("http://bcdn.shandianshipin.cn/rendition-normal/video/%E7%9F%AD%E8%A7%86%E9%A2%91/PUSH/20180812/1533896928_%E5%BB%B6%E7%A6%A7%E6%94%BB%E7%95%A5+%E7%92%8E%E7%8F%9E%E5%BD%93%E4%B8%8A%E8%B4%B5%E4%BA%BA%2C+%E6%95%85%E6%84%8F%E5%81%9A%E8%BF%99%E4%B8%AA%E5%B0%8F%E5%8A%A8%E4%BD%9C%2C+%E8%AE%A9%E4%B9%BE%E9%9A%86%E6%AF%8F%E6%99%9A%E5%8F%AB%E5%A5%B9%E4%BE%8D%E5%AF%9D_%E9%AB%98%E6%B8%85_550.m3u8");
        List<String> urlList = videoTSList.parallelStream()
                .map(VideoTS::getUrl)
                .collect(Collectors.toList());
        System.out.println(urlList);
    }
}
