package com.qiniu.convert;

import com.qiniu.interfaces.IStringFormat;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.*;

public class MapToString extends Converter<Map<String, String>, String> {

    private IStringFormat<Map<String, String>> stringFormatter;

    public MapToString(String format, String separator, List<String> rmFields) throws IOException {
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
    public String convertToV(Map<String, String> line) throws IOException {
        return stringFormatter.toFormatString(line);
    }
}
