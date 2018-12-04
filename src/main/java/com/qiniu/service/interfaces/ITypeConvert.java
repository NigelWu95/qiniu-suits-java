package com.qiniu.service.interfaces;

import java.util.List;

public interface ITypeConvert<T, V> {

    V toV(T t);

    List<V> convertToVList(List<T> srcList);
}