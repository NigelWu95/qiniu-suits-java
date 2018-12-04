package com.qiniu.service.interfaces;

import com.qiniu.storage.model.FileInfo;

import java.util.Map;

public interface IStringFormat {

    default void setSeparator(String separator) {}

    String toFormatString(FileInfo fileInfo, Map<String, Boolean> variablesIfUse);
}