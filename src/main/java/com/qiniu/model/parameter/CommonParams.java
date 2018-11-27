package com.qiniu.model.parameter;

import com.qiniu.config.MainArgs;
import com.qiniu.config.PropertyConfig;
import com.qiniu.util.StringUtils;

import java.io.IOException;

public class CommonParams {

    private MainArgs mainArgs;
    private PropertyConfig propertyConfig;
    private String resultFormat;
    private String resultFileDir;
    private String process = "";
    private String processBatch = "";
    private String maxThreads = "";

    public CommonParams(MainArgs mainArgs) {
        this.mainArgs = mainArgs;
        try { this.resultFormat = getParamFromArgs("result-format"); } catch (Exception e) {}
        try { this.resultFileDir = getParamFromArgs("result-path"); } catch (Exception e) {}
        try { this.process = getParamFromArgs("process"); } catch (Exception e) {}
        try { this.processBatch = getParamFromArgs("process-batch"); } catch (Exception e) {}
        try { this.maxThreads = getParamFromArgs("max-threads"); } catch (Exception e) {}
    }

    public CommonParams(PropertyConfig propertyConfig) {
        this.propertyConfig = propertyConfig;
        try { this.resultFormat = getParamFromConfig("result-format"); } catch (Exception e) {}
        try { this.resultFileDir = getParamFromConfig("result-path"); } catch (Exception e) {}
        try { this.process = getParamFromConfig("process"); } catch (Exception e) {}
        try { this.processBatch = getParamFromConfig("process-batch"); } catch (Exception e) {}
        try { this.maxThreads = getParamFromConfig("max-threads"); } catch (Exception e) {}
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

    public String getResultFormat() {
        if (StringUtils.isNullOrEmpty(resultFormat)) {
            System.out.println("no incorrect result format, it will use \"json\" as default.");
            return "json";
        } else {
            return resultFormat;
        }
    }

    public String getResultFileDir() {
        if (StringUtils.isNullOrEmpty(resultFileDir)) {
            System.out.println("no incorrect result file directory, it will use \"../result\" as default.");
            return "../result";
        }
        return System.getProperty("user.dir") + System.getProperty("file.separator") + resultFileDir;
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
