package com.qiniu.service.convert;

import com.qiniu.service.fileline.JsonLineFormatter;
import com.qiniu.service.interfaces.IStringFormat;
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
    private String format;
    private String separator;
    private IStringFormat stringFormatter;

    public FileInfoToJson(String format, String separator) {
        if ("format".equals(format)) {
            stringFormatter = new JsonLineFormatter();
        } else {
            stringFormatter = new JsonLineFormatter();
        }
    }

    public List<String> convertToVList(List<FileInfo> srcList) {
        Stream<FileInfo> fileInfoStream = srcList.parallelStream().filter(Objects::nonNull);
        return fileInfoStream.map(stringFormatter::toFormatString).collect(Collectors.toList());
    }
}
