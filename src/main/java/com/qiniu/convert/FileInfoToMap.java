package com.qiniu.convert;

import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.LineUtils;

import java.util.*;
import java.util.stream.Collectors;

public class FileInfoToMap implements ITypeConvert<FileInfo, Map<String, String>> {

    private List<String> errorList = new ArrayList<>();

    public List<Map<String, String>> convertToVList(List<FileInfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.stream()
                .map(fileInfo -> {
                    try {
                        return LineUtils.getItemMap(fileInfo);
                    } catch (Exception e) {
                        errorList.add(String.valueOf(fileInfo) + "\t" + e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
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
