package com.qiniu.interfaces;

import java.util.List;

public interface IFileDirLister<E, R> extends ILister<E> {

    String getName();

    List<R> getDirectories();

    List<E> getRemainedFiles();
}
