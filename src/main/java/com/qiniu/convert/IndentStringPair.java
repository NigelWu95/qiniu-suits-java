package com.qiniu.convert;

import com.qiniu.interfaces.KeyValuePair;
import com.qiniu.util.FileUtils;

public class IndentStringPair implements KeyValuePair<String, String> {

    private StringBuilder stringBuilder = new StringBuilder();
    private StringBuilder parentPath = new StringBuilder();
    private String separator;
    private int size;

    public IndentStringPair(String separator) {
        this.separator = separator;
    }

    @Override
    public void putKey(String key, String value) {
        value = value.replace("\n", "%0a").replace("\r", "%0d");
        int num = value.split(FileUtils.pathSeparator).length;
        if (num > 1) {
            if (value.endsWith(FileUtils.pathSeparator)) {
                parentPath.append(value, 0, value.substring(0, value.length() - 1)
                        .lastIndexOf(FileUtils.pathSeparator));
            } else {
                parentPath.append(value, 0, value.lastIndexOf(FileUtils.pathSeparator));
            }
            StringBuilder stringBuilder = new StringBuilder();
            for (int j = 1; j < num; j++) stringBuilder.append("\t");
            stringBuilder.append(value.replace(parentPath, "").substring(1));
            stringBuilder.append(stringBuilder.toString()).append("\t");
            parentPath.delete(0, parentPath.length()).append(value).append("\t|");
        } else {
            stringBuilder.append(value).append("\t");
            parentPath.append(value).append("\t|");
        }
        stringBuilder.append(separator).append(value);
        size++;
    }

    @Override
    public void put(String key, String value) {
        stringBuilder.append(separator).append(value);
        size++;
    }

    @Override
    public void put(String key, Boolean value) {
        stringBuilder.append(separator).append(value);
        size++;
    }

    @Override
    public void put(String key, Integer value) {
        stringBuilder.append(separator).append(value);
        size++;
    }

    @Override
    public void put(String key, Long value) {
        stringBuilder.append(separator).append(value);
        size++;
    }

    @Override
    public String getProtoEntity() {
        return parentPath.append(stringBuilder.deleteCharAt(stringBuilder.length() - 1)).substring(1);
    }

    @Override
    public int size() {
        return size;
    }
}
