package com.qiniu.util;

import com.qiniu.common.QiniuSuitsException;

public class LineUtils {

    public static String getIndexItem(String[] items, int index) throws QiniuSuitsException {
        if (items == null) {
            throw new QiniuSuitsException("line is null.");
        }

        if (items.length < index + 1) {
            throw new QiniuSuitsException("index is out of items' length.");
        }

        return items[index];
    }
}