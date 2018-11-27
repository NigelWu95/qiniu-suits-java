package com.qiniu.model.parameter;

public class LifecycleParams extends QossParams {

    private String days;

    public LifecycleParams(String[] args) throws Exception {
        super(args);
        this.days = getParamFromArgs("days");
    }

    public LifecycleParams(String configFileName) throws Exception {
        super(configFileName);
        this.days = getParamFromConfig("days");
    }

    public int getDays() throws Exception {
        if (days.matches("[\\d]+")) {
            return Integer.valueOf(days);
        } else {
            throw new Exception("no incorrect days, please set it 0 or other number");
        }
    }
}
