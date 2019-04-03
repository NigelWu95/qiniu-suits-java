package com.qiniu.datasource;

import java.io.IOException;
import java.util.List;

public interface ILister<T extends List<E>, E> {

    /**
     * 是否需要进行下一次读取
     * @return 时，表示存在下一次的列表可以读取
     */
    boolean hasNext();

    /**
     * 进行下一次的列表读取
     * @return 返回下一次列表
     * @throws IOException 读取下次列表失败抛出的异常
     */
    T next() throws IOException;

    /**
     * 从 next 的 list 中取出第一个元素
     * @return 返回 next 列表中的第一个元素
     */
    E firstInNext();

    /**
     * 从 next 的 list 中取出最后一个元素
     * @return 返回 next 列表中的最后一个元素
     */
    E lastInNext();

    /**
     * 当 next 抛出异常时检测该状态码
     * @return 异常状态码
     */
    int getStatusCode();

    void setPrefix(String prefix);

    String getPrefix();

    void setMarker(String marker);

    String getMarker();

    void setEndKeyPrefix(String endKeyPrefix);

    String getEndKeyPrefix();

    void setDelimiter(String delimiter);

    String getDelimiter();

    void setLimit(int limit);

    int getLimit();

    /**
     * 关闭掉使用的资源
     */
    void close();
}
