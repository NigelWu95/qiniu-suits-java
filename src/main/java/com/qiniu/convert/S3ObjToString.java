package com.qiniu.convert;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.qiniu.interfaces.IStringFormat;
import com.qiniu.util.LineUtils;

import java.io.IOException;
import java.util.Set;

public class S3ObjToString extends Converter<S3ObjectSummary, String> {

    private IStringFormat<S3ObjectSummary> stringFormatter;

    public S3ObjToString(String format, String separator, Set<String> rmFields) throws IOException {
        if (separator == null) throw new IOException("separator can not be null.");
        if ("json".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, rmFields);
        } else if ("csv".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, ",", rmFields);
        } else if ("tab".equals(format)) {
            stringFormatter = line -> LineUtils.toFormatString(line, separator, rmFields);
        } else {
            throw new IOException("please check your format for map to string.");
        }
    }

    @Override
    public String convertToV(S3ObjectSummary line) throws IOException {
        return stringFormatter.toFormatString(line);
    }
}
