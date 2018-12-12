package com.qiniu.service.interfaces;

import java.util.Map;

public interface IStringFormat<T> {

    String toFormatString(T t, Map<String, Boolean> variablesIfUse);
}
