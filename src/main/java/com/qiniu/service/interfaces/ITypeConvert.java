package com.qiniu.service.interfaces;

import java.util.List;

public interface ITypeConvert<T, V> {

    List<V> convertToVList(List<T> srcList);
}