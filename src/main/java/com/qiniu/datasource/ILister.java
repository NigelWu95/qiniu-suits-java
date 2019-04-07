package com.qiniu.datasource;

import com.qiniu.common.SuitsException;

import java.util.List;

public interface ILister<T extends List<E>, E> {

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

    boolean hasNext();

    void listForward() throws SuitsException;

    T currents();

    E currentFirst();

    E currentLast();

    /**
     * 关闭掉使用的资源
     */
    void close();
}
