package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

public class FileStatusParams extends QossParams {

    private String targetStatus;

    public FileStatusParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        this.targetStatus = entryParam.getParamValue("status");
    }

    public int getTargetStatus() throws Exception {
        if (targetStatus.matches("([01])")) {
            return Short.valueOf(targetStatus);
        } else {
            throw new Exception("no incorrect status, please set it 0 or 1");
        }
    }
}
