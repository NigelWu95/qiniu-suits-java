package com.qiniu.convert;

import com.google.gson.JsonObject;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.*;

public class JsonToString extends Converter<JsonObject, String> {

    private IStringFormat<JsonObject> stringFormatter;

    public JsonToString(String format, String separator, Set<String> rmFields) throws IOException {
        if (separator == null) throw new IOException("separator can not be null.");
        if ("json".equals(format)) {
            stringFormatter = JsonObject::toString;
        } else if ("csv".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, ",", rmFields);
        } else if ("tab".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, separator, rmFields);
        } else {
            throw new IOException("please check your format for map to string.");
        }
    }

    @Override
    public String convertToV(JsonObject line) throws IOException {
        return stringFormatter.toFormatString(line);
    }
}
