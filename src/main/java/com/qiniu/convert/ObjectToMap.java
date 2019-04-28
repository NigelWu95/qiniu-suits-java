package com.qiniu.convert;

import com.qiniu.interfaces.ILineParser;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.*;

public class ObjectToMap<E> implements ITypeConvert<E, Map<String, String>> {

    protected ILineParser<E> lineParser;
    private List<String> errorList = new ArrayList<>();

    public Map<String, String> convertToV(E line) throws IOException {
        return lineParser.getItemMap(line);
    }

    public List<Map<String, String>> convertToVList(List<E> lineList) {
        List<Map<String, String>> mapList = new ArrayList<>();
        if (lineList != null && lineList.size() > 0) {
            for (E line : lineList) {
                try {
                    mapList.add(lineParser.getItemMap(line));
                } catch (Exception e) {
                    errorList.add(JsonConvertUtils.toJson(line) + "\t" + e.getMessage());
                }
            }
        }
        return mapList;
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
