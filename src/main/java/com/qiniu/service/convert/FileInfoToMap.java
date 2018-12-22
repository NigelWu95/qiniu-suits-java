package com.qiniu.service.convert;

import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class FileInfoToMap implements ITypeConvert<FileInfo, Map<String, String>> {

    private List<String> usedFields;
    volatile private List<String> errorList = new ArrayList<>();

    public FileInfoToMap(List<String> usedFields) {
        this.usedFields = usedFields;
    }

    public List<Map<String, String>> convertToVList(List<FileInfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(fileInfo -> {
                    Map<String, String> converted = new HashMap<>();
                    try {
                        usedFields.forEach(key -> {
                            switch (key) {
                                case "key": converted.put(key, fileInfo.key); break;
                                case "fsize": converted.put(key, String.valueOf(fileInfo.fsize)); break;
                                case "putTime": converted.put(key, String.valueOf(fileInfo.putTime)); break;
                                case "mimeType": converted.put(key, fileInfo.mimeType); break;
                                case "endUser": converted.put(key, fileInfo.endUser); break;
                                case "type": converted.put(key, String.valueOf(fileInfo.type)); break;
                                case "status": converted.put(key, String.valueOf(fileInfo.status)); break;
                            }
                        });
                        if (converted.size() < usedFields.size()) throw new IOException();
                        return converted;
                    } catch (Exception e) {
                        errorList.add(String.valueOf(fileInfo));
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public List<String> getErrorList() {
        return errorList;
    }
}