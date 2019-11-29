package com.qiniu.interfaces;

import com.qiniu.common.SuitsException;

import java.util.List;

public interface IStorageLister<E> extends ILister<E> {

    String getPrefix();

    String getMarker();

    boolean hasFutureNext() throws SuitsException;

    String currentEndKey();

    // 默认不使用 directory
    default List<String> getDirectories() {
        return null;
    }
}
