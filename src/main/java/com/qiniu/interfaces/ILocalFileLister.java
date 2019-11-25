package com.qiniu.interfaces;

import java.util.List;

public interface ILocalFileLister<E, D> extends ILister<E> {

    String getName();

    String currentEndFilepath();

    List<D> getDirectories();

    List<E> getRemainedFiles();
}
