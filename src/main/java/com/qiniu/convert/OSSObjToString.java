package com.qiniu.convert;

import com.aliyun.oss.model.OSSObjectSummary;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OSSObjToString extends Converter<OSSObjectSummary, String> {

    private IStringFormat<OSSObjectSummary> stringFormatter;
    private List<String> fields;

    public OSSObjToString(String format, String separator, List<String> rmFields) throws IOException {
        if (separator == null) throw new IOException("separator can not be null.");
        fields = LineUtils.getFields(new ArrayList<String>(){{
            addAll(LineUtils.defaultFileFields);
        }}, new ArrayList<String>(){{
            if (rmFields != null) addAll(rmFields);
            addAll(LineUtils.mimeFields);
            addAll(LineUtils.statusFields);
            addAll(LineUtils.md5Fields);
        }});
        if ("json".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, fields);
        } else if ("csv".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, ",", fields);
        } else if ("tab".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, separator, fields);
        } else {
            throw new IOException("please check your format for map to string.");
        }
    }

    @Override
    public String convertToV(OSSObjectSummary line) throws IOException {
        return stringFormatter.toFormatString(line);
    }
}
