package com.qiniu.convert;

import com.qiniu.interfaces.ILineParser;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class Converter<E, T> implements ITypeConvert<E, T> {

    protected Map<String, String> indexMap;
    protected ILineParser<E> lineParser;
    protected IStringFormat<E> stringFormatter;
    private List<String> errorList = new ArrayList<>();

    Converter() {
    }

    Converter(Map<String, String> indexMap) {
        this.indexMap = indexMap;
    }

    Converter(ILineParser<E> lineParser) {
        this.lineParser = lineParser;
    }

    Converter(IStringFormat<E> stringFormatter) {
        this.stringFormatter = stringFormatter;
    }

    public abstract T convertToV(E line) throws IOException;

    @Override
    public List<T> convertToVList(List<E> lineList) {
        List<T> mapList = new ArrayList<>();
        if (lineList != null && lineList.size() > 0) {
            for (E line : lineList) {
                try {
                    mapList.add(convertToV(line));
                } catch (Exception e) {
                    errorList.add(JsonConvertUtils.toJson(line) + "\t" + e.getMessage());
                }
            }
        }
        return mapList;
    }

    @Override
    public int errorSize() {
        return errorList.size();
    }

    @Override
    public List<String> consumeErrors() {
        try {
            return new ArrayList<>(errorList);
        } finally {
            errorList.clear();
        }
    }
}
