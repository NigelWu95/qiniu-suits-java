package com.qiniu.service.convert;

import com.qiniu.service.fileline.FileInfoJsonFormatter;
import com.qiniu.service.fileline.FileInfoTableFormatter;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.util.*;
import java.util.stream.Collectors;

public class FileInfoToString implements ITypeConvert<FileInfo, String> {

    private IStringFormat<FileInfo> stringFormatter;
    private  List<String> usedFields;
    volatile private List<String> errorList = new ArrayList<>();

    public FileInfoToString(String format, String separator, List<String> usedFields) {
        if ("json".equals(format)) {
            stringFormatter = new FileInfoJsonFormatter();
        } else {
            stringFormatter = new FileInfoTableFormatter(separator);
        }
        this.usedFields = usedFields;
    }

    public List<String> convertToVList(List<FileInfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(fileInfo -> {
                    try {
                        return stringFormatter.toFormatString(fileInfo, usedFields);
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
