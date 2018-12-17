package com.qiniu.service.convert;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.qiniu.model.media.Avinfo;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.storage.model.FileInfo;

import java.util.*;
import java.util.stream.Collectors;

public class AvinfoToString implements ITypeConvert<Avinfo, String> {

    private IStringFormat<Avinfo> stringFormatter;
    volatile private List<String> errorList = new ArrayList<>();

    public AvinfoToString() {
        stringFormatter = (avinfo, variablesIfUse) -> {
            Gson gson = new GsonBuilder().create();
            return gson.toJson(avinfo);
        };
    }

    public List<String> convertToVList(List<Avinfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(avinfo -> {
                    try {
                        return stringFormatter.toFormatString(avinfo, null);
                    } catch (Exception e) {
                        errorList.add(String.valueOf(avinfo));
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
