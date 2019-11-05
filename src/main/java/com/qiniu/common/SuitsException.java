package com.qiniu.common;

import java.io.IOException;

public class SuitsException extends IOException {

    private int statusCode;

    public SuitsException(int statusCode, String error) {
        super(String.join("", "code: ", String.valueOf(statusCode), ", error: ", error));
        this.statusCode = statusCode;
    }

    public SuitsException(Exception e, int statusCode) {
        super(String.join(", ", String.valueOf(statusCode), e.getMessage()), e);
        this.statusCode = statusCode;
    }

    public SuitsException(Exception e, int statusCode, String error) {
        super(String.join(", ", String.valueOf(statusCode), error, e.getMessage()), e);
        this.statusCode = statusCode;
    }

    public SuitsException(SuitsException e, String message) {
        super(String.join(", ", e.getMessage(), message), e);
        this.statusCode = e.getStatusCode();
    }

    public int getStatusCode() {
        return statusCode;
    }
}
