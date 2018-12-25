package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class PfopResultParams extends QossParams {

    private String persistentIdIndex;

    public PfopResultParams(IEntryParam entryParam) throws IOException {
        super(entryParam);
        try { this.persistentIdIndex = entryParam.getParamValue("persistentId-index"); } catch (Exception e) { persistentIdIndex = ""; }
    }

    public String getPersistentIdIndex() throws IOException {
        if ("json".equals(getParseType())) {
            if ("".equals(persistentIdIndex)) {
                throw new IOException("no incorrect json key index for pfop's fops.");
            } else {
                return persistentIdIndex;
            }
        } else if ("table".equals(getParseType())) {
            if ("".equals(persistentIdIndex)) {
                return "0";
            } else if (persistentIdIndex.matches("\\d")) {
                return persistentIdIndex;
            } else {
                throw new IOException("no incorrect persistentId index, it should be a number.");
            }
        } else {
            throw new IOException("no incorrect object key index for pfopresult's persistentIdIndex.");
        }
    }
}
