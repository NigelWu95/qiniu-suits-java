package com.qiniu.util;

import java.util.Random;

public class RandomUtils {

    public static String getRandomLong() {
        return String.valueOf(new Random().nextLong());
    }
}