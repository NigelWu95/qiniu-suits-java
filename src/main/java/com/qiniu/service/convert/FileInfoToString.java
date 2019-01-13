package com.qiniu.service.convert;

import com.qiniu.service.line.FileInfoJsonFormatter;
import com.qiniu.service.line.FileInfoTableFormatter;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.util.*;
import java.util.stream.Collectors;

public class FileInfoToString implements ITypeConvert<FileInfo, String> {

    private IStringFormat<FileInfo> stringFormatter;
    private List<String> errorList = new ArrayList<>();

    public FileInfoToString(String format, String separator, List<String> removeFields) {
        if ("json".equals(format)) {
            stringFormatter = new FileInfoJsonFormatter(removeFields);
        } else {
            stringFormatter = new FileInfoTableFormatter(separator, removeFields);
        }
    }

    public List<String> convertToVList(List<FileInfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        // 使用 parallelStream 时，添加错误行至 errorList 需要同步代码块，stream 时可以直接 errorList.add();
        return srcList.stream()
                .map(fileInfo -> {
                    try {
                        return stringFormatter.toFormatString(fileInfo);
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
