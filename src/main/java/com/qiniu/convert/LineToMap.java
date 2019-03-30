package com.qiniu.convert;

import com.qiniu.interfaces.ILineParser;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class LineToMap implements ITypeConvert<String, Map<String, String>> {

    private ILineParser<String> lineParser;
    private List<String> errorList = new ArrayList<>();

    public LineToMap(String parseType, String separator, Map<String, String> indexMap) throws IOException {
        if ("json".equals(parseType)) {
            this.lineParser = line -> LineUtils.getItemMap(line, indexMap, false);
        } else if ("csv".equals(parseType)) {
            this.lineParser = line -> LineUtils.getItemMap(line, ",", indexMap, false);
        } else if ("tab".equals(parseType)) {
            this.lineParser = line -> LineUtils.getItemMap(line, separator, indexMap, false);
        } else {
            throw new IOException("please check your format for line to map.");
        }
    }

    public Map<String, String> convertToV(String line) throws IOException {
        return lineParser.getItemMap(line);
    }

    public List<Map<String, String>> convertToVList(List<String> lineList) {
        if (lineList == null || lineList.size() == 0) return new ArrayList<>();
        return lineList.stream()
                .map(line -> {
                    try {
                        return lineParser.getItemMap(line);
                    } catch (Exception e) {
                        errorList.add(line + "\t" + e.getMessage());
                        return null;
                    }
                })
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
