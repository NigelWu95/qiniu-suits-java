package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.util.List;

public class InputFieldSaveParams extends ListFieldSaveParams {

    private String fopsSave;
    private String persistentIdSave;

    public InputFieldSaveParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        try { this.fopsSave = entryParam.getParamValue("fops-save"); } catch (Exception e) {}
        try { this.persistentIdSave = entryParam.getParamValue("persistentId-save"); } catch (Exception e) {}
    }

    public List<String> getUsedFields() {
        List<String> usedFields = super.getUsedFields();
        if (fopsSave == null || fopsSave.equals("true")) usedFields.add(9, "fops");
        if (persistentIdSave == null || persistentIdSave.equals("true")) usedFields.add(10, "persistentId");

        return usedFields;
    }
}
