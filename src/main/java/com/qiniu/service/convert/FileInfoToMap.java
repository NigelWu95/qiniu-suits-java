package com.qiniu.service.convert;

import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.util.*;
import java.util.stream.Collectors;

public class FileInfoToMap implements ITypeConvert<FileInfo, Map<String, String>> {

    private Map<String, Boolean> variablesIfUse;

    public FileInfoToMap() {
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
//        variablesIfUse.put("status", status);
    }

    public Map<String, String> toV(FileInfo fileInfo) {
        Map<String, String> converted = new HashMap<>();
        variablesIfUse.forEach((key, value) -> {
            if (value) {
                switch (key) {
                    case "key": converted.put(key, fileInfo.key); break;
                    case "fsize": converted.put(key, String.valueOf(fileInfo.fsize)); break;
                    case "putTime": converted.put(key, String.valueOf(fileInfo.putTime)); break;
                    case "mimeType": converted.put(key, fileInfo.mimeType); break;
                    case "endUser": converted.put(key, fileInfo.endUser); break;
                    case "type": converted.put(key, String.valueOf(fileInfo.type)); break;
//                    case "status": converted.put(key, fileInfo.status); break;
                }
            }
        });
        return converted;
    }

    public List<Map<String, String>> convertToVList(List<FileInfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(this::toV)
                .collect(Collectors.toList());
    }
}