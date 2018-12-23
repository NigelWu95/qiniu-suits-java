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
    private  List<String> usedFields;

    public FileInfoToString(String format, String separator, List<String> usedFields) throws IOException {
        if (usedFields == null || usedFields.size() == 0) throw new IOException("there are no fields be set.");
        this.usedFields = usedFields;
        if ("json".equals(format)) {
            stringFormatter = new FileInfoJsonFormatter();
        } else {
            stringFormatter = new FileInfoTableFormatter(separator);
        }
    }

    public List<String> convertToVList(List<FileInfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(fileInfo -> stringFormatter.toFormatString(fileInfo, usedFields))
                .collect(Collectors.toList());
    }
}
