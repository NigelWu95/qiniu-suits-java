package com.qiniu.service.interfaces;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public interface ITypeConvert<T, V> {

    List<V> convertToVList(List<T> srcList) throws IOException;

    default List<String> getErrorList() {
        return new ArrayList<>();
    }
}
