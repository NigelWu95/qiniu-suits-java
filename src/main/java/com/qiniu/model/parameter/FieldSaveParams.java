package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FieldSaveParams extends CommonParams {

    private String keySave;
    private String hashSave;
    private String fsizeSave;
    private String putTimeSave;
    private String mimeTypeSave;
    private String endUserSave;
    private String typeSave;
    private String statusSave;

    public FieldSaveParams(IEntryParam entryParam) throws IOException {
        super(entryParam);
        try { this.keySave = entryParam.getParamValue("key-save"); } catch (Exception e) {}
        try { this.hashSave = entryParam.getParamValue("hash-save"); } catch (Exception e) {}
        try { this.fsizeSave = entryParam.getParamValue("fsize-save"); } catch (Exception e) {}
        try { this.putTimeSave = entryParam.getParamValue("putTime-save"); } catch (Exception e) {}
        try { this.mimeTypeSave = entryParam.getParamValue("mimeType-save"); } catch (Exception e) {}
        try { this.endUserSave = entryParam.getParamValue("endUser-save"); } catch (Exception e) {}
        try { this.typeSave = entryParam.getParamValue("type-save"); } catch (Exception e) {}
        try { this.statusSave = entryParam.getParamValue("status-save"); } catch (Exception e) {}
    }

    public List<String> getUsedFields() {
        List<String> usedFields = new ArrayList<>();
        if (keySave == null || keySave.equals("true")) usedFields.add("key");
        if (hashSave == null || hashSave.equals("true")) usedFields.add("hash");
        if (fsizeSave == null || fsizeSave.equals("true")) usedFields.add("fsize");
        if (putTimeSave == null || putTimeSave.equals("true")) usedFields.add("putTime");
        if (mimeTypeSave == null || mimeTypeSave.equals("true")) usedFields.add("mimeType");
        if (endUserSave == null || endUserSave.equals("true")) usedFields.add("endUser");
        if (typeSave == null || typeSave.equals("true")) usedFields.add("type");
        if (statusSave == null || statusSave.equals("true")) usedFields.add("status");

        return usedFields;
    }
}
