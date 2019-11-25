package com.qiniu.interfaces;

import java.util.List;

public interface IDirectoryLister<E, R> extends IList<E> {

    String getName();

    List<R> getDirectories();
}
