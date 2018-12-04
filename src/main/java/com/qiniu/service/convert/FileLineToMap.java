package com.qiniu.service.convert;

import com.qiniu.service.fileline.JsonLineParser;
import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.service.interfaces.ILineParser;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.*;
import java.util.stream.Collectors;

public class FileLineToMap implements ITypeConvert<String, Map<String, String>> {

    private ILineParser lineParser;

    public FileLineToMap(String parserTye, String separator) {
        if ("json".equals(parserTye)) {
            lineParser = new JsonLineParser();
        } else {
            lineParser = new SplitLineParser(separator);
        }
    }

    public boolean filterLine(String line) {
        // TODO add filter method
        return true;
    }

    public Map<String, String> toV(String line) {
        return lineParser.getItemMap(line);
    }

    public List<Map<String, String>> convertToVList(List<String> srcList) {
        return srcList.parallelStream()
                .filter(line -> line != null && !"".equals(line))
                .map(this::toV)
                .collect(Collectors.toList());
    }
}
