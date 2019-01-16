package com.qiniu.service.convert;

import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.service.line.MapToJsonFormatter;
import com.qiniu.service.line.MapToTableFormatter;

import java.util.*;
import java.util.stream.Collectors;

public class MapToString implements ITypeConvert<Map<String, String>, String> {

    private IStringFormat<Map<String, String>> stringFormatter;

    public MapToString(String format, String separator, List<String> removeFields) {
        List<String> rmFields = removeFields == null ? new ArrayList<>() : removeFields;
        // 将 file info 的字段逐一进行获取是为了控制输出字段的顺序
        if ("json".equals(format)) {
            stringFormatter = new MapToJsonFormatter(rmFields);
        } else {
            stringFormatter = new MapToTableFormatter(separator, rmFields);
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
