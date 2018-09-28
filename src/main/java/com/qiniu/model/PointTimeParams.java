package com.qiniu.model;

import com.qiniu.util.DateUtils;
import com.qiniu.util.StringUtils;

public class PointTimeParams extends BaseParams {

    private String pointDate;
    private String pointTime;
    private String direction;

    public PointTimeParams(String[] args) throws Exception {
        super(args);
        try { this.pointDate = getParamFromArgs("date"); } catch (Exception e) {}
        try { this.pointTime = getParamFromArgs("time"); } catch (Exception e) {}
        try { this.direction = getParamFromArgs("direction"); } catch (Exception e) {}
    }

    public PointTimeParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.pointDate = getParamFromConfig("date"); } catch (Exception e) {}
        try { this.pointTime = getParamFromConfig("time"); } catch (Exception e) {}
        try { this.direction = getParamFromConfig("direction"); } catch (Exception e) {}
    }

    public String getPointDate() {
        if (StringUtils.isNullOrEmpty(pointDate) || !pointDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
            System.out.println("no incorrect date, it will use current date as default.");
            return DateUtils.getCurrentDatetime()[0];
        } else {
            return pointDate;
        }
    }

    public String getPointTime() {
        if (StringUtils.isNullOrEmpty(pointDate) || !pointTime.matches("\\d{2}:\\d{2}:\\d{2}")) {
            System.out.println("no incorrect time, it will use 00:00:00 as default.");
            return "00:00:00";
        } else {
            return pointTime;
        }
    }

    public boolean getDirection() {
        if (StringUtils.isNullOrEmpty(pointDate) || !direction.matches("\\d")) {
            System.out.println("no incorrect direction, it will use 0(pre) as default.");
            return true;
        } else {
            return Integer.valueOf(direction) == 0;
        }
    }
}