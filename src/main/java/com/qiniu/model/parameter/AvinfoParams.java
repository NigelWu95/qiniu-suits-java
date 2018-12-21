package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

public class AvinfoParams extends QossParams {

    private String domain;
    private String https;
    private String needSign;

    public AvinfoParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        this.domain = entryParam.getParamValue("domain");
        try { this.https = entryParam.getParamValue("use-https"); } catch (Exception e) { https = ""; }
        try { this.needSign = entryParam.getParamValue("need-sign"); } catch (Exception e) { needSign = ""; }
    }

    public String getDomain() {
        return domain;
    }

    public boolean getHttps() {
        if (https.matches("(true|false)")) {
            return Boolean.valueOf(https);
        } else {
            System.out.println("no incorrect use-https, it will use false as default.");
            return false;
        }
    }

    public boolean getNeedSign() {
        if (needSign.matches("(true|false)")) {
            return Boolean.valueOf(needSign);
        } else {
            System.out.println("no incorrect need-sign, it will use false as default.");
            return false;
        }
    }
}
