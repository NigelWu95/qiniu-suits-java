package com.qiniu.model.parameter;

public class FileStatParams extends QossParams {

    private String domain;
    private String https = "";
    private String needSign = "";

    public FileStatParams(String[] args) throws Exception {
        super(args);
        this.domain = getParamFromArgs("domain");
        try { this.https = getParamFromArgs("use-https"); } catch (Exception e) {}
        try { this.needSign = getParamFromArgs("need-sign"); } catch (Exception e) {}
    }

    public FileStatParams(String configFileName) throws Exception {
        super(configFileName);
        this.domain = getParamFromConfig("domain");
        try { this.https = getParamFromConfig("use-https"); } catch (Exception e) {}
        try { this.needSign = getParamFromConfig("need-sign"); } catch (Exception e) {}
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
