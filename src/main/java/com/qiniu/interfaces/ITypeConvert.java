package com.qiniu.interfaces;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public interface ITypeConvert<E, T> {

    T convertToV(E line) throws IOException;

    List<T> convertToVList(List<E> srcList);

    int errorSize();

    // 消费者方法，调用后现有的错误记录列表转换成 String 并清空列表
    default String errorLines() {
        return null;
    }
}
