package com.qiniu.service.convert;

import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.LineUtils;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileInfoToJson implements ITypeConvert<FileInfo, String> {

    private Map<String, Boolean> variablesIfUse;
    private List<String> fieldList;

    public List<String> convertToVList(List<FileInfo> srcList) {
        Stream<FileInfo> fileInfoStream = srcList.parallelStream().filter(Objects::nonNull);
        return fileInfoStream.map(JsonConvertUtils::toJsonWithoutUrlEscape).collect(Collectors.toList());
    }
}