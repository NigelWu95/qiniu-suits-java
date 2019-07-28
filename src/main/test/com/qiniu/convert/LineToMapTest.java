package com.qiniu.convert;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class LineToMapTest {

    @Test
    public void test() {
        MapToString mapToString1;
        MapToString mapToString2;
        try {
            mapToString1 = new MapToString("csv", "\t", null, null);
            mapToString2 = new MapToString("csv", "\t", null, null);
            mapToString1.convertToVList(new ArrayList<Map<String, String>>(){{
                add(new HashMap<>());
                add(new HashMap<>());
            }});
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
