package com.qiniu.util;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.model.FileInfo;

public class LineUtils {

    public static String getIndexItem(String[] items, int index) throws QiniuException {
        if (items == null) {
            throw new QiniuException(null, "line is null.");
        }

        if (items.length < index + 1) {
            throw new QiniuException(null, "index is out of items' length.");
        }

        return items[index];
    }

    public static String toSeparatedItemLine(FileInfo fileInfo) {

        return fileInfo.key + "\t" + fileInfo.hash + "\t" + fileInfo.fsize + "\t" + fileInfo.putTime
                + "\t" + fileInfo.type;
    }
}
