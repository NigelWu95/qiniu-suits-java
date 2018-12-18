package com.qiniu.model.parameter;

import java.util.List;

public class InputFieldParams extends ListFieldParams {

    private String fopsSave;
    private String persistentIdSave;

    public InputFieldParams(String[] args) throws Exception {
        super(args);
        try { this.fopsSave = getParamFromArgs("fops-save"); } catch (Exception e) {}
        try { this.persistentIdSave = getParamFromArgs("persistentId-save"); } catch (Exception e) {}
    }

    public InputFieldParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.fopsSave = getParamFromConfig("fops-save"); } catch (Exception e) {}
        try { this.persistentIdSave = getParamFromConfig("persistentId-save"); } catch (Exception e) {}
    }

    public List<String> getUsedFields() {
        List<String> usedFields = super.getUsedFields();
        if (fopsSave == null || fopsSave.equals("true")) usedFields.add(9, "fops");
        if (persistentIdSave == null || persistentIdSave.equals("true")) usedFields.add(10, "persistentId");

        return usedFields;
    }
}
