package com.qiniu.convert;

import com.qiniu.interfaces.IStringFormat;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class ObjectToString<E> implements ITypeConvert<E, String> {

    protected IStringFormat<E> stringFormatter;
    protected List<String> errorList = new ArrayList<>();

    public String convertToV(E line) throws IOException {
        return stringFormatter.toFormatString(line);
    }

    public List<String> convertToVList(List<E> lineList) {
        List<String> stringList = new ArrayList<>();
        if (lineList != null && lineList.size() > 0) {
            for (E line : lineList) {
                try {
                    stringList.add(stringFormatter.toFormatString(line));
                } catch (Exception e) {
                    errorList.add(JsonConvertUtils.toJson(line) + "\t" + e.getMessage());
                }
            }
        }
        return stringList;
    }

    public int errorSize() {
        return errorList.size();
    }

    public List<String> consumeErrors() {
        try {
            return new ArrayList<>(errorList);
        } finally {
            errorList.clear();
        }
    }
}
