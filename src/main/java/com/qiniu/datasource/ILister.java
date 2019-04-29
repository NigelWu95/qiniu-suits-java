package com.qiniu.datasource;

import com.qiniu.common.SuitsException;

import java.util.List;

public interface ILister<E> {

    void setPrefix(String prefix);

    String getPrefix();

    void setMarker(String marker);

    String getMarker();

    void setEndPrefix(String endPrefix);

    String getEndPrefix();

    void setDelimiter(String delimiter);

    String getDelimiter();

    void setLimit(int limit);

    int getLimit();

    void setStraight(boolean straight);

    boolean canStraight();

    void listForward() throws SuitsException;

    boolean hasNext();

    boolean hasFutureNext() throws SuitsException;

    List<E> currents();

    E currentLast();

    String currentLastKey();

    void updateMarkerBy(E object);

    /**
     * 关闭掉使用的资源
     */
    void close();
}
