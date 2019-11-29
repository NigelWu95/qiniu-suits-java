package com.qiniu.datasource;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DefaultFileContainerTest {

    @Test
    public void test() {
        String path = "../";
        Map<String, String> indexMap = new HashMap<String, String>(){{
            put("key", "key");
        }};
        try {
            DefaultFileContainer defaultFileContainer = new DefaultFileContainer(path, null, null,
                    true, indexMap, null, 10, 10);
            defaultFileContainer.setSaveOptions(true, "../result", "tab", "\t", null);
            defaultFileContainer.export();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}