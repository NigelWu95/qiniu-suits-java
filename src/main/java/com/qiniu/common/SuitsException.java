package com.qiniu.common;

import java.io.IOException;

public class SuitsException extends IOException {

    private int statusCode;
    private String error;

    public SuitsException(int statusCode, String error) {
        super("code: " + statusCode + ", error: " + error);
        this.statusCode = statusCode;
        this.error = error;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getError() {
        return error;
    }
}
