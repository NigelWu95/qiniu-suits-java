package com.qiniu.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QiniuSuitsException extends Exception {

    private String exceptionType;
    private Map<String, String> fieldMap;

    public QiniuSuitsException(Exception e) {
        this.fieldMap = new HashMap<String, String>();
        if (e != null) {
            this.fieldMap.put("msg", e.getMessage());
            this.exceptionType = "QiniuSuitsException from " + e.toString().split(":")[0];
        } else {
            this.exceptionType = "QiniuSuitsException";
        }
    }

    public QiniuSuitsException(String msg) {
        this.fieldMap = new HashMap<String, String>();
        this.fieldMap.put("msg", msg);
        this.exceptionType = "QiniuSuitsException";
    }

    public void setStackTrace(StackTraceElement[] stackTrace) {
        super.setStackTrace(stackTrace);
    }

    public StackTraceElement[] getStackTrace() {
        return super.getStackTrace();
    }

    public void printStackTrace() {
        super.printStackTrace();
    }

    public void addToFieldMap(String key, String object) {
        this.fieldMap.put(key, object);
    }

    public String getField(String key) {
        if (this.fieldMap.containsKey(key)) {
            return this.fieldMap.get(key);
        } else {
            return "null";
        }
    }

    public String getMessage() {
        StringBuilder stringBuilder = new StringBuilder("{");
        List<String> fieldList = new ArrayList<>();
        fieldList.addAll(fieldMap.keySet());

        for (int i = 0; i < fieldList.size(); i++) {
            if (i == fieldList.size() - 1) {
                stringBuilder.append("\"" + fieldList.get(i) + "\":\"" + fieldMap.get(fieldList.get(i)) + "\"}");
            } else {
                stringBuilder.append("\"" + fieldList.get(i) + "\":\"" + fieldMap.get(fieldList.get(i)) + "\",");
            }
        }

        return stringBuilder.toString();
    }

    public String toString() {
        return exceptionType + ": " + getMessage();
    }
}