package com.qiniu.datasource;

import com.qiniu.common.SuitsException;

import java.util.List;

public interface ILister<E> {

    String getBucket();

    String getPrefix();

    void setMarker(String marker);

    String getMarker();

    void setEndPrefix(String endPrefix);

    String getEndPrefix();

    void setLimit(int limit);

    int getLimit();

    void listForward() throws SuitsException;

    boolean hasNext();

    boolean hasFutureNext() throws SuitsException;

    List<E> currents();

    String currentEndKey();

    String truncate();

    /**
     * 关闭掉使用的资源
     */
    void close();
}
