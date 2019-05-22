package com.qiniu.convert;

import com.qiniu.interfaces.IStringFormat;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class QOSObjToString extends Converter<FileInfo, String> {

    private IStringFormat<FileInfo> stringFormatter;

    public QOSObjToString(String format, String separator, Set<String> rmFields) throws IOException {
        if (separator == null) throw new IOException("separator can not be null.");
        if ("json".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, rmFields);
        } else if ("csv".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, ",", rmFields);
        } else if ("tab".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, separator, rmFields);
        } else {
            throw new IOException("please check your format for map to string.");
        }
    }

    @Override
    public String convertToV(FileInfo line) throws IOException {
        return stringFormatter.toFormatString(line);
    }
}
