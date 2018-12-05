package com.qiniu.service.convert;

import com.qiniu.model.media.Avinfo;
import com.qiniu.service.fileline.AvinfoJsonFormatter;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.service.interfaces.ITypeConvert;

import java.util.*;
import java.util.stream.Collectors;

public class AvinfoToString implements ITypeConvert<Avinfo, String> {

    private IStringFormat<Avinfo> stringFormatter;

    public AvinfoToString() {
        stringFormatter = new AvinfoJsonFormatter();
    }

    public String toV(Avinfo avinfo) {

        return stringFormatter.toFormatString(avinfo);
    }

    public List<String> convertToVList(List<Avinfo> srcList) {
        if (srcList == null || srcList.size() == 0) return new ArrayList<>();
        return srcList.parallelStream()
                .filter(Objects::nonNull)
                .map(this::toV)
                .collect(Collectors.toList());
    }
}
