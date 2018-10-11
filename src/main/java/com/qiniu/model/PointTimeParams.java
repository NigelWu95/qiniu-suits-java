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

    public String getPointDatetime() {
        if (StringUtils.isNullOrEmpty(pointDate) && StringUtils.isNullOrEmpty(pointDate)) {
            return null;
        } else if(pointDate.matches("\\d{4}-\\d{2}-\\d{2}") && pointTime.matches("\\d{2}:\\d{2}:\\d{2}")) {
            return pointDate + " " + pointTime;
        } else {
            System.out.println("no incorrect date or time, it will use current datetime as default.");
            return DateUtils.getCurrentDatetime()[0] + " " + DateUtils.getCurrentDatetime()[1];
        }
    }

    public boolean getDirection() {
        if (StringUtils.isNullOrEmpty(direction) || !direction.matches("\\d")) {
            System.out.println("no incorrect direction, it will use 0(pre) as default. it will not take effect if datetime is empty.");
            return true;
        } else {
            return Integer.valueOf(direction) == 0;
        }
    }
}