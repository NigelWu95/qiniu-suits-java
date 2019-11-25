package com.qiniu.interfaces;

import com.qiniu.common.SuitsException;

public interface IPrefixLister<E> extends ILister<E> {

    String getBucket();

    String getPrefix();

    void setMarker(String marker);

    String getMarker();

    boolean hasFutureNext() throws SuitsException;
}
