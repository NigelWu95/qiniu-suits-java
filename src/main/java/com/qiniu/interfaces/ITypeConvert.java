package com.qiniu.interfaces;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public interface ITypeConvert<E, V> {

    V convertToV(E line) throws IOException;

    List<V> convertToVList(List<E> srcList);

    int errorSize();

    // 消费者方法，调用后消费错误记录列表
    default List<String> consumeErrors() {
        return new ArrayList<>();
    }
}
