package com.qiniu.convert;

import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.*;

public class MapToString extends Converter<Map<String, String>, String> {

    public MapToString(String format, String separator, List<String> rmFields) throws IOException {
        // 将 file info 的字段逐一进行获取是为了控制输出字段的顺序
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
