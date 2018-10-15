package com.qiniu.model;

public class LifecycleParams extends BaseParams {

    private String days;
    private PointTimeParams pointTimeParams;

    public LifecycleParams(String[] args) throws Exception {
        super(args);
        pointTimeParams = new PointTimeParams(args);
        this.days = getParamFromArgs("days");
    }

    public LifecycleParams(String configFileName) throws Exception {
        super(configFileName);
        pointTimeParams = new PointTimeParams(configFileName);
        this.days = getParamFromConfig("days");
    }

    public int getDays() throws Exception {
        if (days.matches("[\\d]+")) {
            return Integer.valueOf(days);
        } else {
            throw new Exception("the days is incorrect, please set it 0 or other number");
        }
    }

    public PointTimeParams getPointTimeParams() {
        return pointTimeParams;
    }
}