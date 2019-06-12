package com.qiniu.process.filtration;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SeniorFilterTest {

    private SeniorFilter<Map<String, String>> seniorFilter;

    @Test
    @Before
    public void init() throws IOException {
        seniorFilter = new SeniorFilter<Map<String, String>>("ext-mime", "resources/check-config.json", false) {
            @Override
            protected String valueFrom(Map<String, String> item, String key) {
                return item != null ? item.get(key) : null;
            }
        };
    }

    @Test
    public void testCheckMimeType() {
        System.out.println(seniorFilter.checkMimeType(
                new HashMap<String, String>(){{
                    put("key", "tset.mp4");
                    put("mimeType", "text/html");
                }}
        ));
        System.out.println(seniorFilter.checkMimeType(
                new HashMap<String, String>(){{
                    put("key", "test.mp4");
                    put("mimeType", "video/mp4");
                }}
        ));
        System.out.println(seniorFilter.checkMimeType(
                new HashMap<String, String>(){{
                    put("key", "test.mp5");
                    put("mimeType", "video/mp5");
                }}
        ));
    }
}
