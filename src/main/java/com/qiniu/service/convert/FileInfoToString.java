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
    private  List<String> fields;
    volatile private List<String> errorList = new ArrayList<>();

    public FileInfoToString(String format, String separator, List<String> fields) throws IOException {
        if (fields == null || fields.size() == 0) throw new IOException("there are no fields be set.");
        this.fields = fields;
        if ("json".equals(format)) {
            stringFormatter = new FileInfoJsonFormatter();
        } else {
            stringFormatter = new FileInfoTableFormatter(separator);
        }
    }

    public List<String> convertToVList(List<FileInfo> srcList) throws IOException {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        List<String> resultList = srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(fileInfo -> {
                    try {
                        return stringFormatter.toFormatString(fileInfo, fields);
                    } catch (Exception e) {
                        errorList.add(fileInfo.key + "\t" + String.valueOf(fileInfo));
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (errorList.size() == srcList.size()) throw new IOException("covert map by fields failed, " +
                "please check the save fields' setting.");
        return resultList;
    }

    public List<String> getErrorList() {
        return errorList;
    }
}
