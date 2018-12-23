package com.qiniu.service.convert;

import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.util.*;
import java.util.stream.Collectors;

public class FileInfoToMap implements ITypeConvert<FileInfo, Map<String, String>> {

    private List<String> fields;

    public FileInfoToMap() {
        this.fields = new ArrayList<>();
        fields.add(0, "key");
        fields.add(1, "hash");
        fields.add(2, "fsize");
        fields.add(3, "putTime");
        fields.add(4, "mimeType");
        fields.add(5, "endUser");
        fields.add(6, "type");
        fields.add(7, "status");
        fields.add(8, "md5");
    }

    public List<Map<String, String>> convertToVList(List<FileInfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(fileInfo -> {
                    Map<String, String> converted = new HashMap<>();
                    fields.forEach(key -> {
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
