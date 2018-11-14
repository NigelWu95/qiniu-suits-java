package com.qiniu.util;

import com.qiniu.common.QiniuSuitsException;
import com.qiniu.storage.model.FileInfo;

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

    public static String toSeparatedItemLine(FileInfo fileInfo) {

        return fileInfo.key + "\t" + fileInfo.hash + "\t" + fileInfo.fsize + "\t" + fileInfo.putTime
                + "\t" + fileInfo.type;
    }
}