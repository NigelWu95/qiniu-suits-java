package com.qiniu.service.datasource;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListBucketTest {

    @Test
    public void setPrefixesScope() {
        Map<Boolean, List<String>> map = new HashMap<Boolean, List<String>>(){{
            put(true, new ArrayList<String>(){{add("true");}});
            put(false, new ArrayList<String>(){{add("false");}});
        }};
        List<String> list = new ArrayList<>();
        list.addAll(map.get(true));
        System.out.println(list);
//        map = new HashMap<Boolean, List<String>>(){{
//            put(true, new ArrayList<String>(){{add("true-new");}});
//            put(false, new ArrayList<String>(){{add("false-new");}});
//        }};
        map.put(true, new ArrayList<String>(){{add("true-new");}});
        list.addAll(map.get(true));
        System.out.println(list);
    }
}