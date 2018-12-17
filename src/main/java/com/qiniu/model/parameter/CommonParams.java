package com.qiniu.model.parameter;

import com.qiniu.config.MainArgs;
import com.qiniu.config.PropertyConfig;

import java.io.IOException;

public class CommonParams {

    private MainArgs mainArgs;
    private PropertyConfig propertyConfig;
    private String unitLen;
    private String resultFileDir;
    private String resultFormat;
    private String resultSeparator;
    protected String saveTotal;
    private String process;
    private String processBatch = "";
    private String maxThreads = "";

    public CommonParams(MainArgs mainArgs) {
        this.mainArgs = mainArgs;
        try { this.unitLen = getParamFromArgs("unit-len"); } catch (Exception e) { unitLen = ""; }
        try { this.resultFileDir = getParamFromArgs("result-path"); } catch (Exception e) {}
        try { this.resultFormat = getParamFromArgs("result-format"); } catch (Exception e) {}
        try { this.resultSeparator = getParamFromArgs("result-separator"); } catch (Exception e) {}
        try { this.saveTotal = getParamFromArgs("save-total"); } catch (Exception e) { saveTotal = ""; }
        try { this.process = getParamFromArgs("process"); } catch (Exception e) {}
        try { this.processBatch = getParamFromArgs("process-batch"); } catch (Exception e) { processBatch = ""; }
        try { this.maxThreads = getParamFromArgs("max-threads"); } catch (Exception e) { maxThreads = ""; }
    }

    public CommonParams(PropertyConfig propertyConfig) {
        this.propertyConfig = propertyConfig;
        try { this.unitLen = getParamFromConfig("unit-len"); } catch (Exception e) { unitLen = ""; }
        try { this.resultFileDir = getParamFromConfig("result-path"); } catch (Exception e) {}
        try { this.resultFormat = getParamFromConfig("result-format"); } catch (Exception e) {}
        try { this.resultSeparator = getParamFromConfig("result-separator"); } catch (Exception e) {}
        try { this.saveTotal = getParamFromConfig("save-total"); } catch (Exception e) { saveTotal = ""; }
        try { this.process = getParamFromConfig("process"); } catch (Exception e) {}
        try { this.processBatch = getParamFromConfig("process-batch"); } catch (Exception e) { processBatch = ""; }
        try { this.maxThreads = getParamFromConfig("max-threads"); } catch (Exception e) { maxThreads = ""; }
    }

    public CommonParams(String[] args) throws IOException {
        this(new MainArgs(args));
    }

    public CommonParams(String configFileName) throws IOException {
        this(new PropertyConfig(configFileName));
    }

    public String getParamFromArgs(String key) throws IOException {
        return mainArgs.getParamValue(key);
    }

    public String getParamFromConfig(String key) throws IOException {
        return propertyConfig.getProperty(key);
    }

    public String getParamByKey(String key) throws IOException {
        if (mainArgs != null) return mainArgs.getParamValue(key);
        else if (propertyConfig != null) return propertyConfig.getProperty(key);
        return "";
    }

    public int getUnitLen() {
        if (unitLen.matches("\\d+")) {
            return Integer.valueOf(unitLen);
        } else {
            System.out.println("no incorrect unit-len, it will use 1000 as default.");
            return 1000;
        }
    }

    public String getResultFileDir() {
        if (resultFileDir == null || "".equals(resultFileDir)) {
            System.out.println("no incorrect result file directory, it will use \"../result\" as default.");
            return "../result";
        }
        return System.getProperty("user.dir") + System.getProperty("file.separator") + resultFileDir;
    }

    public String getResultFormat() {
        if (resultFormat == null || "".equals(resultFormat)) {
            System.out.println("no incorrect result format, it will use \"json\" as default.");
            return "json";
        } else {
            return resultFormat;
        }
    }

    public String getResultSeparator() {
        if (resultSeparator == null || "".equals(resultSeparator)) {
            System.out.println("no incorrect result separator, it will use \"\t\" as default.");
            return "\t";
        } else {
            return resultSeparator;
        }
    }

    public String getProcess() {
        return process;
    }

    public boolean getProcessBatch() {
        if (processBatch.matches("(true|false)")) {
            return Boolean.valueOf(processBatch);
        } else {
            System.out.println("no incorrect process-batch status, it will use true as default.");
            return true;
        }
    }

    public int getMaxThreads() {
        if (maxThreads.matches("[1-9]\\d*")) {
            return Integer.valueOf(maxThreads);
        } else {
            System.out.println("no incorrect threads, it will use 10 as default.");
            return 10;
        }
    }
}
