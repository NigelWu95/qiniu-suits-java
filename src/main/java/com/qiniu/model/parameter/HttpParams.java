package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

public class HttpParams {

    private String connectTimeout;
    private String readTimeout;
    private String writeTimeout;

    public HttpParams(IEntryParam entryParam) {
        try { this.connectTimeout = entryParam.getParamValue("connect-timeout"); } catch (Exception e) { connectTimeout = ""; }
        try { this.readTimeout = entryParam.getParamValue("read-timeout"); } catch (Exception e) { readTimeout = ""; }
        try { this.writeTimeout = entryParam.getParamValue("write-timeout"); } catch (Exception e) { writeTimeout = ""; }
    }

    public int getConnectTimeout() {
        if (connectTimeout.matches("\\d+")) {
            return Integer.valueOf(connectTimeout);
        } else {
            return 30;
        }
    }

    public int getReadTimeout() {
        if (readTimeout.matches("\\d+")) {
            return Integer.valueOf(readTimeout);
        } else {
            return 60;
        }
    }

    public int getWriteTimeout() {
        if (writeTimeout.matches("\\d+")) {
            return Integer.valueOf(writeTimeout);
        } else {
            return 10;
        }
    }
}
