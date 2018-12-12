package com.qiniu.service.convert;

import com.qiniu.model.parameter.InfoMapParams;
import com.qiniu.service.fileline.JsonLineParser;
import com.qiniu.service.fileline.SplitLineParser;
import com.qiniu.service.interfaces.ILineParser;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.*;
import java.util.stream.Collectors;

public class FileLineToMap implements ITypeConvert<String, Map<String, String>> {

    private ILineParser lineParser;

    public FileLineToMap(String parserTye, String separator, InfoMapParams infoMapParams) {

        Map<String, String> infoIndexMap = new HashMap<>();
        infoIndexMap.put(infoMapParams.getKeyIndex(), "key");
        infoIndexMap.put(infoMapParams.getHashIndex(), "hash");
        infoIndexMap.put(infoMapParams.getFsizeIndex(), "fsize");
        infoIndexMap.put(infoMapParams.getPutTimeIndex(), "putTime");
        infoIndexMap.put(infoMapParams.getMimeTypeIndex(), "mimeType");
        infoIndexMap.put(infoMapParams.getEndUserIndex(), "endUser");
        infoIndexMap.put(infoMapParams.getTypeIndex(), "type");
        infoIndexMap.put(infoMapParams.getStatusIndex(), "status");
        infoIndexMap.put(infoMapParams.getMd5Index(), "md5");
        infoIndexMap.put(infoMapParams.getFopsIndex(), "fops");
        infoIndexMap.put(infoMapParams.getPersistentIdIndex(), "persistentId");

        if ("json".equals(parserTye)) {
            lineParser = new JsonLineParser(infoIndexMap);
        } else {
            lineParser = new SplitLineParser(separator, infoIndexMap);
        }
    }

    public List<Map<String, String>> convertToVList(List<String> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(line -> line != null && !"".equals(line))
                .map(line -> lineParser.getItemMap(line))
                .collect(Collectors.toList());
    }
}
