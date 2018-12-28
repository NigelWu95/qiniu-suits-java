package com.qiniu.service.convert;

import com.qiniu.service.fileline.FileInfoJsonFormatter;
import com.qiniu.service.fileline.FileInfoTableFormatter;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class FileInfoToString implements ITypeConvert<FileInfo, String> {

    private IStringFormat<FileInfo> stringFormatter;
    volatile private List<String> errorList = new ArrayList<>();

    public FileInfoToString(String format, String separator, List<String> rmFields) {
        if ("json".equals(format)) {
            stringFormatter = new FileInfoJsonFormatter(rmFields);
        } else {
            stringFormatter = new FileInfoTableFormatter(separator, rmFields);
        }
    }

    public List<String> convertToVList(List<FileInfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(fileInfo -> {
                    if (fileInfo == null) {
                        errorList.add("empty fileInfo.");
                        return false;
                    } else return true;
                })
                .map(stringFormatter::toFormatString)
                .collect(Collectors.toList());
    }

    public List<String> getErrorList() {
        return errorList;
    }
}
