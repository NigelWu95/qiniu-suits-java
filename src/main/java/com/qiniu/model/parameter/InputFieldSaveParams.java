package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;
import java.util.List;

public class InputFieldSaveParams extends ListFieldSaveParams {

    private String fopsSave;
    private String persistentIdSave;
    private String newKeySave;
    private String urlSave;

    public InputFieldSaveParams(IEntryParam entryParam) throws IOException {
        super(entryParam);
        try { this.fopsSave = entryParam.getParamValue("fops-save"); } catch (Exception e) {}
        try { this.persistentIdSave = entryParam.getParamValue("persistentId-save"); } catch (Exception e) {}
        try { this.newKeySave = entryParam.getParamValue("newKey-save"); } catch (Exception e) {}
        try { this.urlSave = entryParam.getParamValue("url-save"); } catch (Exception e) {}
    }

    public List<String> getUsedFields() {
        List<String> usedFields = super.getUsedFields();
        if (fopsSave == null || fopsSave.equals("true")) usedFields.add("fops");
        if (persistentIdSave == null || persistentIdSave.equals("true")) usedFields.add("persistentId");
        if (newKeySave == null || newKeySave.equals("true")) usedFields.add("newKey");
        if (urlSave == null || urlSave.equals("true")) usedFields.add("url");

        return usedFields;
    }
}
