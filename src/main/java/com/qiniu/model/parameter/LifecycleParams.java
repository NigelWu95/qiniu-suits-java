package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

public class LifecycleParams extends QossParams {

    private String days;

    public LifecycleParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        days = entryParam.getParamValue("days");
    }

    public int getDays() throws Exception {
        if (days.matches("[\\d]+")) {
            return Integer.valueOf(days);
        } else {
            throw new Exception("no incorrect days, please set it 0 or other number.");
        }
    }
}
