package com.qiniu.service.convert;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.qiniu.model.media.PfopResult;
import com.qiniu.model.qoss.Qhash;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.*;
import java.util.stream.Collectors;

public class QhashToString implements ITypeConvert<Qhash, String> {

    private IStringFormat<Qhash> stringFormatter;
    volatile private List<String> errorList = new ArrayList<>();

    public QhashToString(String format, String separator) {
        if ("format".equals(format)) {
            stringFormatter = (qhash, variablesIfUse) -> {
                Gson gson = new GsonBuilder().create();
                return gson.toJson(qhash);
            };
        } else {
            stringFormatter = (qhash, variablesIfUse) ->
                    String.valueOf(qhash.hash) + separator + String.valueOf(qhash.fsize) + separator;
        }
    }

    public List<String> convertToVList(List<Qhash> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(qhash -> {
                    try {
                        return stringFormatter.toFormatString(qhash, null);
                    } catch (Exception e) {
                        errorList.add(String.valueOf(qhash));
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
