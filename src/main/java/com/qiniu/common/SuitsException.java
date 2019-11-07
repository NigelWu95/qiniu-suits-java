package com.qiniu.common;

import java.io.IOException;

public class SuitsException extends IOException {

    private int statusCode;
    private Exception exception;

    public SuitsException(int statusCode, String error) {
        super(String.join("", "code: ", String.valueOf(statusCode), ", error: ", error));
        this.statusCode = statusCode;
    }

    public SuitsException(Exception e, int statusCode) {
        super(String.join(", ", String.valueOf(statusCode), e.getMessage()));
        this.exception = e;
        this.statusCode = statusCode;
    }

    public SuitsException(Exception e, int statusCode, String error) {
        super(String.join(", ", String.valueOf(statusCode), error, e.getMessage()));
        this.exception = e;
        this.statusCode = statusCode;
    }

    public SuitsException(SuitsException e, String message) {
        super(String.join(", ", e.getMessage(), message));
        this.exception = e;
        this.statusCode = e.getStatusCode();
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Exception getSuperException() {
        return exception;
    }
}
