package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

public class FileTypeParams extends QossParams {

    private String targetType;

    public FileTypeParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        this.targetType = entryParam.getParamValue("type");
    }

    public int getTargetType() throws Exception {
        if (targetType.matches("([01])")) {
            return Integer.valueOf(targetType);
        } else {
            throw new Exception("no incorrect type, please set it 0 or 1.");
        }
    }
}
