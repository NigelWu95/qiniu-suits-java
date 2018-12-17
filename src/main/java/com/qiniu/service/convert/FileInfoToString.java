package com.qiniu.service.convert;

import com.qiniu.model.qoss.Qhash;
import com.qiniu.service.fileline.FileInfoJsonFormatter;
import com.qiniu.service.fileline.FileInfoTableFormatter;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.util.*;
import java.util.stream.Collectors;

public class FileInfoToString implements ITypeConvert<FileInfo, String> {

    private IStringFormat<FileInfo> stringFormatter;
    private Map<String, Boolean> variablesIfUse;

    public FileInfoToString(String format, String separator, boolean hash, boolean fsize, boolean putTime,
                            boolean mimeType, boolean endUser, boolean type, boolean status) {
        if ("format".equals(format)) {
            stringFormatter = new FileInfoJsonFormatter();
        } else {
            stringFormatter = new FileInfoTableFormatter(separator);
        }
        variablesIfUse = new HashMap<>();
        variablesIfUse.put("key", true);
        variablesIfUse.put("hash", hash);
        variablesIfUse.put("fsize", fsize);
        variablesIfUse.put("putTime", putTime);
        variablesIfUse.put("mimeType", mimeType);
        variablesIfUse.put("endUser", endUser);
        variablesIfUse.put("type", type);
//        variablesIfUse.put("status", status);
    }

    public List<String> convertToVList(List<FileInfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        List<FileInfo> errorList = new ArrayList<>();
        List<String> successList = srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(fileInfo -> {
                    try {
                        return stringFormatter.toFormatString(fileInfo, variablesIfUse);
                    } catch (Exception e) {
                        errorList.add(fileInfo);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        srcList = errorList;
        return successList;
    }
}
