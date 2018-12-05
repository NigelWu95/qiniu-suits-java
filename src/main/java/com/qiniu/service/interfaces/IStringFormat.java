package com.qiniu.service.interfaces;

import java.util.Map;

public interface IStringFormat<T> {

    default String toFormatString(T t, Map<String, Boolean> variablesIfUse) {
        return null;
    }

    default String toFormatString(T t) {
        return null;
    }
}