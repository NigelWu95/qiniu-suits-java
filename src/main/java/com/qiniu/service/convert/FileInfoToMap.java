package com.qiniu.service.convert;

import com.qiniu.model.qoss.Qhash;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.util.*;
import java.util.stream.Collectors;

public class FileInfoToMap implements ITypeConvert<FileInfo, Map<String, String>> {

    private Map<String, Boolean> variablesIfUse;

    public FileInfoToMap(boolean hash, boolean fsize, boolean putTime, boolean mimeType, boolean endUser, boolean type,
                         boolean status) {
        variablesIfUse = new HashMap<>();
        variablesIfUse.put("key", true);
        variablesIfUse.put("hash", hash);
        variablesIfUse.put("fsize", fsize);
        variablesIfUse.put("putTime", putTime);
        variablesIfUse.put("mimeType", mimeType);
        variablesIfUse.put("endUser", endUser);
        variablesIfUse.put("type", type);
//        variablesIfUse.put("status", status);
    }

    public List<Map<String, String>> convertToVList(List<FileInfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        List<FileInfo> errorList = new ArrayList<>();
        List<Map<String, String>> successList = srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(fileInfo -> {
                    Map<String, String> converted = new HashMap<>();
                    try {
                        variablesIfUse.forEach((key, value) -> {
                            if (value) {
                                switch (key) {
                                    case "key": converted.put(key, fileInfo.key); break;
                                    case "fsize": converted.put(key, String.valueOf(fileInfo.fsize)); break;
                                    case "putTime": converted.put(key, String.valueOf(fileInfo.putTime)); break;
                                    case "mimeType": converted.put(key, fileInfo.mimeType); break;
                                    case "endUser": converted.put(key, fileInfo.endUser); break;
                                    case "type": converted.put(key, String.valueOf(fileInfo.type)); break;
//                                case "status": converted.put(key, fileInfo.status); break;
                                }
                            }
                        });
                        return converted;
                    } catch (Exception e) {
                        errorList.add(fileInfo);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        srcList = errorList;
        return successList;
    }
}