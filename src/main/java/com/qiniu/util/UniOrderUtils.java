package com.qiniu.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class UniOrderUtils {

    private static ConcurrentMap<Integer, Integer> orderMap = new ConcurrentHashMap<>();
    private static AtomicInteger order = new AtomicInteger(0);

    public static int getOrder() {
        Integer ord = order.addAndGet(1);
        Integer rem = ord % 5000;
        if (ord > 5000 && rem == 0) rem = 5000;
        while (ord > 5000) {
            if (orderMap.remove(rem) != null) ord = rem;
        }
        return ord;
    }

    public static void returnOrder(int order) {
        orderMap.put(order, order);
    }
}
