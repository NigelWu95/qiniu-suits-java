package com.qiniu.datasource;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class QiniuQosContainerTest {

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

        ConcurrentMap<String, Map<String, String>> concurrentMap = new ConcurrentHashMap<>();
        concurrentMap.put("a", new HashMap<String, String>(){{ put("a", "a"); }});
        System.out.println(concurrentMap.remove("a"));
        System.out.println(concurrentMap);

        Map<String, Map<String, String>> inMap = new HashMap<>();
        Map<String, String> sMap = new HashMap<String, String>(){{ put("a", "a"); }};
        inMap.put("1", sMap);
        sMap = new HashMap<>();
        sMap.put("b", "b");
        System.out.println(inMap);
    }
}
