package com.qiniu.service.fileline;

import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileInfoTableFormatter implements IStringFormat<FileInfo> {

    private String separator;
    private List<String> rmFields;

    public FileInfoTableFormatter(String separator, List<String> removeFields) {
        this.separator = separator;
        this.rmFields = removeFields == null ? new ArrayList<>() : removeFields;
    }

    public String toFormatString(FileInfo fileInfo) {
        StringBuilder converted = new StringBuilder();
        if (!rmFields.contains("key")) converted.append(fileInfo.key).append(separator);
        if (!rmFields.contains("hash")) converted.append(fileInfo.hash).append(separator);
        if (!rmFields.contains("fsize")) converted.append(fileInfo.fsize).append(separator);
        if (!rmFields.contains("putTime")) converted.append(fileInfo.putTime).append(separator);
        if (!rmFields.contains("mimeType")) converted.append(fileInfo.mimeType).append(separator);
        if (!rmFields.contains("endUser")) converted.append(fileInfo.endUser).append(separator);
        if (!rmFields.contains("type")) converted.append(fileInfo.type).append(separator);
        if (!rmFields.contains("status")) converted.append(fileInfo.status).append(separator);
        return converted.toString();
    }
}
