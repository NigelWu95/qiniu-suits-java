package com.qiniu.interfaces;

import java.util.List;

public interface IStorageDirLister<E, R> extends ILister<E> {

    String getBucket();

    String getPrefix();

    void setMarker(String marker);

    String getMarker();

    List<R> getDirectories();
}
