package com.qiniu.service.interfaces;

import com.qiniu.storage.model.FileInfo;

import java.util.Map;

public interface IStringFormat {

    String toFormatString(FileInfo fileInfo, Map<String, Boolean> variablesIfUse);
}