package com.qiniu.util;

import java.util.Map;

public class LineUtils {

    static public boolean checkItem(Map<String, String> item, String key) {
        return item == null || item.get(key) == null || "".equals(item.get(key));
    }
}
