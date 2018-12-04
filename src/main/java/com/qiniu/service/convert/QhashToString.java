package com.qiniu.service.convert;

import com.qiniu.model.qoss.Qhash;
import com.qiniu.service.fileline.QhashJsonFormatter;
import com.qiniu.service.fileline.QhashTableFormatter;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QhashToString implements ITypeConvert<Qhash, String> {

    private IStringFormat<Qhash> stringFormatter;
    private Map<String, Boolean> variablesIfUse;

    public QhashToString(String format, String separator) {
        if ("format".equals(format)) {
            stringFormatter = new QhashJsonFormatter();
        } else {
            stringFormatter = new QhashTableFormatter(separator);
        }
        variablesIfUse = new HashMap<>();
        variablesIfUse.put("hash", true);
        variablesIfUse.put("fsize", true);
    }

    public void chooseVariables(boolean hash, boolean fsize) {
        variablesIfUse.put("hash", hash);
        variablesIfUse.put("fsize", fsize);
    }

    public String toV(Qhash qhash) {

        return stringFormatter.toFormatString(qhash, variablesIfUse);
    }

    public List<String> convertToVList(List<Qhash> srcList) {
        Stream<Qhash> fileInfoStream = srcList.parallelStream().filter(Objects::nonNull);
        return fileInfoStream.map(this::toV).collect(Collectors.toList());
    }
}
