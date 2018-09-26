package com.qiniu.model;

import com.qiniu.util.DateUtils;

public class PointTimeParams extends BaseParams {

    private String pointDate;
    private String pointTime;
    private String direction;

    public PointTimeParams(String[] args) throws Exception {
        super(args);
        this.pointDate = getParam("date");
        this.pointTime = getParam("time");
        this.direction = getParam("direction");
    }

    public String getPointDate() {
        if (pointDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return pointDate;
        } else {
            System.out.println("the date is incorrect, it will use current date as default.");
            return DateUtils.getCurrentDatetime()[0];
        }
    }

    public String getPointTime() {
        if (pointTime.matches("\\d{2}:\\d{2}:\\d{2}")) {
            return pointTime;
        } else {
            System.out.println("the time is incorrect, it will use 00:00:00 as default.");
            return "00:00:00";
        }
    }

    public boolean getDirection() {
        if (direction.matches("\\d")) {
            return Integer.valueOf(direction) == 0;
        } else {
            System.out.println("the direction is incorrect, it will use 0(pre) as default.");
            return true;
        }
    }
}