package com.qiniu.service.convert;

import com.qiniu.service.fileline.JsonLineParser;
import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.service.interfaces.ILineParser;
import com.qiniu.service.interfaces.ITypeConvert;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class LineToInfoMap implements ITypeConvert<String, Map<String, String>> {

    private ILineParser lineParser;
    volatile private List<String> errorList = new ArrayList<>();

    public LineToInfoMap(String parseType, String separator, Map<String, String> infoIndexMap) throws IOException {
        if (infoIndexMap == null || infoIndexMap.size() == 0) throw new IOException("there are no indexes be set.");
        if ("json".equals(parseType)) {
            this.lineParser = new JsonLineParser(infoIndexMap);
        } else {
            this.lineParser = new SplitLineParser(separator, infoIndexMap);
        }
    }

    public List<Map<String, String>> convertToVList(List<String> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
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
}
