package com.qiniu.service.convert;

import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class FileInfoToMap implements ITypeConvert<FileInfo, Map<String, String>> {

    private List<String> usedFields;

    public FileInfoToMap(List<String> usedFields) throws IOException {
        if (usedFields == null || usedFields.size() == 0) throw new IOException("there are no fields be set.");
        this.usedFields = usedFields;
    }

    public List<Map<String, String>> convertToVList(List<FileInfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(fileInfo -> {
                    Map<String, String> converted = new HashMap<>();
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
                    return converted;
                })
                .collect(Collectors.toList());
    }
}
