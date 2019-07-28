package com.qiniu.convert;

import com.qiniu.interfaces.IStringFormat;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QOSObjToString extends Converter<FileInfo, String> {

    private IStringFormat<FileInfo> stringFormatter;
    private List<String> fields;

    public QOSObjToString(String format, String separator, List<String> rmFields) throws IOException {
        if (separator == null) throw new IOException("separator can not be null.");
        fields = LineUtils.getFields(new ArrayList<>(LineUtils.defaultFileFields), rmFields);
        if ("json".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, fields);
        } else if ("csv".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, ",", fields);
        } else if ("tab".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, separator, fields);
        } else {
            throw new IOException("please check your format for map to string.");
        }
    }

    @Override
    public String convertToV(FileInfo line) throws IOException {
        return stringFormatter.toFormatString(line);
    }
}
