package com.qiniu.interfaces;

import com.qiniu.sdk.FileInfo;

import java.util.List;

public interface IFilter {

    boolean doFileFilter(FileInfo fileInfo);

    default boolean checkList(List<String> list) {
        return list != null && list.size() != 0;
    }

    boolean isValid();
}