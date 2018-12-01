package com.qiniu.service.convert;

import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

public class FileInfoConverter implements ITypeConvert<FileInfo, Map<String, String>> {

    private Map<String, Boolean> variablesIfUse;
    private List<String> fieldList;

    public FileInfoConverter() {
        variablesIfUse = new HashMap<>();
        variablesIfUse.put("key", true);
        Field[] fields = FileInfo.class.getFields();
        Arrays.asList(fields).forEach(field -> this.fieldList.add(field.getName()));
    }

    public FileInfoConverter(boolean hash, boolean fsize, boolean putTime, boolean mimeType, boolean endUser,
                             boolean type, boolean status) {
        this();
        variablesIfUse.put("hash", hash);
        variablesIfUse.put("fsize", fsize);
        variablesIfUse.put("putTime", putTime);
        variablesIfUse.put("mimeType", mimeType);
        variablesIfUse.put("endUser", endUser);
        variablesIfUse.put("type", type);
        variablesIfUse.put("status", status);
    }

    public Map<String, String> fileInfoToMap(FileInfo fileInfo) {
        Map<String, String> converted = new HashMap<>();
        variablesIfUse.forEach((key, value) -> {
            if (value) {
                if ("key".equals(key)) converted.put(key, fileInfo.key);
                else if ("fsize".equals(key)) converted.put(key, String.valueOf(fileInfo.fsize));
                else if ("putTime".equals(key)) converted.put(key, String.valueOf(fileInfo.putTime));
                else if ("mimeType".equals(key)) converted.put(key, fileInfo.mimeType);
                else if ("endUser".equals(key)) converted.put(key, fileInfo.endUser);
                else if ("type".equals(key)) converted.put(key, String.valueOf(fileInfo.type));
//                else if ("status".equals(key)) converted.put(key, fileInfo.status);
            }
        });
        return converted;
    }

    public boolean filterFileInfo(FileInfo fileInfo) {
        // TODO add filter method
        return true;
    }

    public List<Map<String, String>> convertToVList(List<FileInfo> srcList) {
        return srcList.parallelStream()
                .filter(Objects::nonNull)
                .filter(this::filterFileInfo)
                .map(this::fileInfoToMap)
                .collect(Collectors.toList());
    }
}