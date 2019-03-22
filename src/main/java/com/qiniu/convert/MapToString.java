package com.qiniu.convert;

import com.qiniu.interfaces.IStringFormat;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.line.MapToJsonFormatter;
import com.qiniu.line.MapToTableFormatter;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class MapToString implements ITypeConvert<Map<String, String>, String> {

    private IStringFormat<Map<String, String>> stringFormatter;

    public MapToString(String format, String separator, List<String> rmFields) throws IOException {
        // 将 file info 的字段逐一进行获取是为了控制输出字段的顺序
        if ("json".equals(format)) {
            stringFormatter = new MapToJsonFormatter(rmFields);
        } else if ("csv".equals(format)) {
            stringFormatter = new MapToTableFormatter(",", rmFields);
        } else if ("tab".equals(format)) {
            stringFormatter = new MapToTableFormatter(separator, rmFields);
        } else {
            throw new IOException("please check your format for map to string.");
        }
    }

    public List<String> convertToVList(List<Map<String, String>> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        // 使用 parallelStream 时，添加错误行至 errorList 需要同步代码块，stream 时可以直接 errorList.add();
        return srcList.stream()
                .map(stringFormatter::toFormatString)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
