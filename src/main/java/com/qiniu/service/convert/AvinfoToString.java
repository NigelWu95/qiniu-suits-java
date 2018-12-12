package com.qiniu.service.convert;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.qiniu.model.media.Avinfo;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.*;
import java.util.stream.Collectors;

public class AvinfoToString implements ITypeConvert<Avinfo, String> {

    private IStringFormat<Avinfo> stringFormatter;

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
                .map(avinfo -> stringFormatter.toFormatString(avinfo, null))
                .collect(Collectors.toList());
    }
}
