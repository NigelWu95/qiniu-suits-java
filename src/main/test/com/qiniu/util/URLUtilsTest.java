package com.qiniu.util;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class URLUtilsTest {

    @Test
    public void testGetKey() throws IOException {
        System.out.println(URLUtils.getKey("http://xxx.com/akljshfdsk.txt"));
        System.out.println(URLUtils.getKey("http://xxx.com/akljshf dsk.txt"));
        System.out.println(URLUtils.getKey("http://xxx.com/akljsh\tfdsk.txt"));
        System.out.println(URLUtils.getKey("http://xxx.com/akljshfdsk.txt\t"
                .replace(" ", "%20").replace("\t", "%09")));
        System.out.println(URLUtils.getKey("http://xxx.com/akljshfdsk.txt %09"));
        System.out.println(URLUtils.getKey("http://xxx.com/akljshfdsk.txt\t%09"));
        System.out.println(URLUtils.getKey("http://xxx.com"));
        System.out.println(URLUtils.getKey("http://xxx.com/wordSplit/xml/20161220/FF8080815919A15101591AFE37C603F7\t"));
    }
}