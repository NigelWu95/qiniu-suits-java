package com.qiniu.convert;

import com.qiniu.interfaces.IStringFormat;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class MapToString implements ITypeConvert<Map<String, String>, String> {

    private IStringFormat<Map<String, String>> stringFormatter;
    private List<String> errorList = new ArrayList<>();

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

    public List<String> convertToVList(List<Map<String, String>> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.stream()
                .map(stringFormatter::toFormatString)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public List<String> getErrorList() {
        return errorList;
    }

    public List<String> consumeErrorList() {
        List<String> errors = new ArrayList<>();
        Collections.addAll(errors, new String[errorList.size()]);
        Collections.copy(errors, errorList);
        errorList.clear();
        return errors;
    }
}
