package com.qiniu.convert;

import com.google.gson.JsonObject;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.*;

public class StatJsonToString extends Converter<JsonObject, String> {

    private IStringFormat<JsonObject> stringFormatter;
    private List<String> fields;

    public StatJsonToString(String format, String separator, List<String> rmFields) throws IOException {
        if (separator == null) throw new IOException("separator can not be null.");
        fields = LineUtils.getFields(new ArrayList<String>(){{
            add("key");
            add("hash");
            add("fsize");
            add("putTime");
            add("mimeType");
            add("type");
            add("status");
            add("md5");
            add("endUser");
            add("_id");
        }}, rmFields);
        if ("json".equals(format)) {
            stringFormatter = JsonObject::toString;
        } else if ("csv".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, ",", fields);
        } else if ("tab".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, separator, fields);
        } else {
            throw new IOException("please check your format for map to string.");
        }
    }

    @Override
    public String convertToV(JsonObject line) throws IOException {
        return stringFormatter.toFormatString(line);
    }
}
