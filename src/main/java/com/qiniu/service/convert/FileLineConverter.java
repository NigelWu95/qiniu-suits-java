package com.qiniu.service.convert;

import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.service.interfaces.ILineParser;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.*;
import java.util.stream.Collectors;

public class FileLineConverter implements ITypeConvert<String, Map<String, String>> {

    private ILineParser lineParser;

    public FileLineConverter(String parserTye, String separator) {
        if ("json".equals(parserTye)) {}
        else {
            lineParser = new SplitLineParser(separator);
        }
    }

    public boolean filterLine(String line) {
        // TODO add filter method
        return true;
    }

    public List<Map<String, String>> convertToVList(List<String> srcList) {
        return srcList.parallelStream()
                .map(lineParser::getItemMap)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}