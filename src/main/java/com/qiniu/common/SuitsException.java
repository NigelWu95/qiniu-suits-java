package com.qiniu.common;

import java.io.IOException;

public class SuitsException extends IOException {

    private int statusCode;

    public SuitsException(Exception e, int statusCode) {
        super(statusCode + ", " + e.getMessage(), e);
        this.statusCode = statusCode;
    }

    public SuitsException(int statusCode, String error) {
        super("code: " + statusCode + ", error: " + error);
        this.statusCode = statusCode;
    }

    public SuitsException(Exception e, int statusCode, String error) {
        super(statusCode + ", " + error + ", " + e.getMessage(), e);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
