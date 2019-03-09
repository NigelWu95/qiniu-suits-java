package com.qiniu.service.filtration;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

public class SeniorCheckerTest {

    private SeniorChecker seniorChecker;

    @Test
    @Before
    public void init() throws IOException {
        seniorChecker = new SeniorChecker("mime", "resources/.check.json", false);
    }

    @Test
    public void testCheckMimeType() {
        System.out.println(seniorChecker.checkMimeType(
                new HashMap<String, String>(){{
                    put("key", "tset.mp4");
                    put("mimeType", "text/html");
                }}
        ));
        System.out.println(seniorChecker.checkMimeType(
                new HashMap<String, String>(){{
                    put("key", "test.mp4");
                    put("mimeType", "video/mp4");
                }}
        ));
        System.out.println(seniorChecker.checkMimeType(
                new HashMap<String, String>(){{
                    put("key", "test.mp5");
                    put("mimeType", "video/mp5");
                }}
        ));
    }
}
