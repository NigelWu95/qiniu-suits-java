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

    private String format;
    private String separator;
    private IStringFormat stringFormatter;
    private Map<String, Boolean> variablesIfUse;

    public FileInfoToJson(String format, String separator) {
        if ("format".equals(format)) {
            stringFormatter = new JsonLineFormatter();
        } else {
            stringFormatter = new JsonLineFormatter();
        }
        variablesIfUse = new HashMap<>();
        variablesIfUse.put("key", true);
    }

    public void chooseVariables(boolean hash, boolean fsize, boolean putTime, boolean mimeType, boolean endUser,
                         boolean type, boolean status) {
        variablesIfUse.put("hash", hash);
        variablesIfUse.put("fsize", fsize);
        variablesIfUse.put("putTime", putTime);
        variablesIfUse.put("mimeType", mimeType);
        variablesIfUse.put("endUser", endUser);
        variablesIfUse.put("type", type);
        variablesIfUse.put("status", status);
    }

    public String toJson(FileInfo fileInfo) {

        return stringFormatter.toFormatString(fileInfo, variablesIfUse);
    }

    public List<String> convertToVList(List<FileInfo> srcList) {
        Stream<FileInfo> fileInfoStream = srcList.parallelStream().filter(Objects::nonNull);
        return fileInfoStream.map(this::toJson).collect(Collectors.toList());
    }
}
