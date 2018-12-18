package com.qiniu.model.parameter;

public class QhashParams extends QossParams {

    private String domain;
    private String algorithm;
    private String https;
    private String needSign;

    public QhashParams(String[] args) throws Exception {
        super(args);
        this.domain = getParamFromArgs("domain");
        try { this.algorithm = getParamFromArgs("algorithm"); } catch (Exception e) { algorithm = ""; }
        try { this.https = getParamFromArgs("use-https"); } catch (Exception e) { https = ""; }
        try { this.needSign = getParamFromArgs("need-sign"); } catch (Exception e) { needSign = ""; }
    }

    public QhashParams(String configFileName) throws Exception {
        super(configFileName);
        this.domain = getParamFromConfig("domain");
        try { this.algorithm = getParamFromConfig("algorithm"); } catch (Exception e) { algorithm = ""; }
        try { this.https = getParamFromConfig("use-https"); } catch (Exception e) { https = ""; }
        try { this.needSign = getParamFromConfig("need-sign"); } catch (Exception e) { needSign = ""; }
    }

    public String getDomain() {
        return domain;
    }

    public String getAlgorithm() {
        if (algorithm.matches("(md5|sha1)")) {
            return algorithm;
        } else {
            System.out.println("no incorrect algorithm, it will use \"md5\" as default.");
            return "md5";
        }
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

    public boolean needOptions() {
        return !"".equals(algorithm) || !"".equals(https) || !"".equals(needSign);
    }
}
