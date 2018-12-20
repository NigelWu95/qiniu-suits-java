package com.qiniu.model.parameter;

import java.util.ArrayList;
import java.util.List;

public class ListFieldSaveParams extends CommonParams {

    private String keySave;
    private String hashSave;
    private String fsizeSave;
    private String putTimeSave;
    private String mimeTypeSave;
    private String endUserSave;
    private String typeSave;
    private String statusSave;
    private String md5Save;

    public ListFieldSaveParams(String[] args) throws Exception {
        super(args);
        try { this.keySave = getParamFromArgs("key-save"); } catch (Exception e) {}
        try { this.hashSave = getParamFromArgs("hash-save"); } catch (Exception e) {}
        try { this.fsizeSave = getParamFromArgs("fsize-save"); } catch (Exception e) {}
        try { this.putTimeSave = getParamFromArgs("putTime-save"); } catch (Exception e) {}
        try { this.mimeTypeSave = getParamFromArgs("mimeType-save"); } catch (Exception e) {}
        try { this.endUserSave = getParamFromArgs("endUser-save"); } catch (Exception e) {}
        try { this.typeSave = getParamFromArgs("type-save"); } catch (Exception e) {}
        try { this.statusSave = getParamFromArgs("status-save"); } catch (Exception e) {}
        try { this.md5Save = getParamFromArgs("md5-save"); } catch (Exception e) {}
    }

    public ListFieldSaveParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.keySave = getParamFromConfig("key-save"); } catch (Exception e) {}
        try { this.hashSave = getParamFromConfig("hash-save"); } catch (Exception e) {}
        try { this.fsizeSave = getParamFromConfig("fsize-save"); } catch (Exception e) {}
        try { this.putTimeSave = getParamFromConfig("putTime-save"); } catch (Exception e) {}
        try { this.mimeTypeSave = getParamFromConfig("mimeType-save"); } catch (Exception e) {}
        try { this.endUserSave = getParamFromConfig("endUser-save"); } catch (Exception e) {}
        try { this.typeSave = getParamFromConfig("type-save"); } catch (Exception e) {}
        try { this.statusSave = getParamFromConfig("status-save"); } catch (Exception e) {}
        try { this.md5Save = getParamFromConfig("md5-save"); } catch (Exception e) {}
    }

    public List<String> getUsedFields() {
        List<String> usedFields = new ArrayList<>();
        if (keySave == null || keySave.equals("true")) usedFields.add(0, "key");
        if (hashSave == null || hashSave.equals("true")) usedFields.add(1, "hash");
        if (fsizeSave == null || fsizeSave.equals("true")) usedFields.add(2, "fsize");
        if (putTimeSave == null || putTimeSave.equals("true")) usedFields.add(3, "putTime");
        if (mimeTypeSave == null || mimeTypeSave.equals("true")) usedFields.add(4, "mimeType");
        if (endUserSave == null || endUserSave.equals("true")) usedFields.add(5, "endUser");
        if (typeSave == null || typeSave.equals("true")) usedFields.add(6, "type");
        if (statusSave == null || statusSave.equals("true")) usedFields.add(7, "status");
        if (md5Save == null || md5Save.equals("true")) usedFields.add(8, "md5");

        return usedFields;
    }
}
