package com.qiniu.service.convert;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.qiniu.model.media.PfopResult;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PfopResultToString implements ITypeConvert<PfopResult, String> {

    private IStringFormat<PfopResult> stringFormatter;
    volatile private List<String> errorList = new ArrayList<>();

    public PfopResultToString() {
        stringFormatter = (pfopResult, variablesIfUse) -> {
            Gson gson = new GsonBuilder().create();
            return gson.toJson(pfopResult);
        };
    }

    public List<String> convertToVList(List<PfopResult> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(pfopResult -> {
                    try {
                        return stringFormatter.toFormatString(pfopResult, null);
                    } catch (Exception e) {
                        errorList.add(String.valueOf(pfopResult));
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
