package com.qiniu.convert;

import com.qiniu.interfaces.IStringFormat;
import com.qiniu.interfaces.ITypeConvert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class ObjectToString<E> implements ITypeConvert<E, String> {

    protected IStringFormat<E> stringFormatter;
    protected List<String> errorList = new ArrayList<>();

    public String convertToV(E line) throws IOException {
        return stringFormatter.toFormatString(line);
    }

    public List<String> convertToVList(List<E> lineList) {
        if (lineList == null || lineList.size() == 0) return new ArrayList<>();
        return lineList.stream()
                .map(line -> {
                    try {
                        return stringFormatter.toFormatString(line);
                    } catch (IOException e) {
                        errorList.add(String.valueOf(line) + "\t" + e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(ArrayList::new));
    }

    public List<String> getErrorList() {
        return errorList;
    }

    public List<String> consumeErrorList() {
        List<String> errors = new ArrayList<>();
        Collections.addAll(errors, new String[errorList.size()]);
        Collections.copy(errors, errorList);
        errorList.clear();
        return errors;
    }
}
