package com.qiniu.service.convert;

import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.util.*;
import java.util.stream.Collectors;

public class FileInfoToMap implements ITypeConvert<FileInfo, Map<String, String>> {

    private List<String> fields;
    private List<String> errorList = new ArrayList<>();

    public FileInfoToMap() {
        this.fields = new ArrayList<>();
        fields.add("key");
        fields.add("hash");
        fields.add("fsize");
        fields.add("putTime");
        fields.add("mimeType");
        fields.add("endUser");
        fields.add("type");
        fields.add("status");
    }

    public FileInfoToMap(List<String> fields) {
        this.fields = fields == null ? new ArrayList<>() : fields;
    }

    public List<Map<String, String>> convertToVList(List<FileInfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        // 使用 parallelStream 时，添加错误行至 errorList 需要同步代码块，stream 时可以直接 errorList.add();
        return srcList.stream()
                .map(fileInfo -> {
                    try {
                        Map<String, String> converted = new HashMap<>();
                        fields.forEach(key -> {
                            switch (key) {
                                case "key": converted.put(key, fileInfo.key); break;
                                case "hash": converted.put(key, fileInfo.hash); break;
                                case "fsize": converted.put(key, String.valueOf(fileInfo.fsize)); break;
                                case "putTime": converted.put(key, String.valueOf(fileInfo.putTime)); break;
                                case "mimeType": converted.put(key, fileInfo.mimeType); break;
                                case "endUser": converted.put(key, fileInfo.endUser); break;
                                case "type": converted.put(key, String.valueOf(fileInfo.type)); break;
                                case "status": converted.put(key, String.valueOf(fileInfo.status)); break;
                            }
                        });
                        return converted;
                    } catch (Exception e) {
                        addError(String.valueOf(fileInfo) + "\t" + e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    synchronized private void addError(String errorLine) {
        errorList.add(errorLine);
    }

    public List<String> getErrorList() {
        return errorList;
    }

    public List<String> consumeErrorList() {
        List<String> errors = new ArrayList<>();
        Collections.addAll(errors, new String[errorList.size()]);
        Collections.copy(errors, errorList);
        errorList.clear();
        return errors;
    }
}
