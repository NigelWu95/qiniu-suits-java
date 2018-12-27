package com.qiniu.service.fileline;

import com.google.gson.JsonObject;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileInfoJsonFormatter implements IStringFormat<FileInfo> {

    private List<String> rmFields;

    public FileInfoJsonFormatter(List<String> rmFields) {
        this.rmFields = rmFields == null ? new ArrayList<>() : rmFields;
    }

    public String toFormatString(FileInfo fileInfo) throws IOException {
        JsonObject converted = new JsonObject();
        if (!rmFields.contains("key")) converted.addProperty("key", fileInfo.key);
        if (!rmFields.contains("hash")) converted.addProperty("hash", fileInfo.hash);
        if (!rmFields.contains("fsize")) converted.addProperty("fsize", fileInfo.fsize);
        if (!rmFields.contains("putTime")) converted.addProperty("putTime", fileInfo.putTime);
        if (!rmFields.contains("mimeType")) converted.addProperty("mimeType", fileInfo.mimeType);
        if (!rmFields.contains("endUser")) converted.addProperty("endUser", fileInfo.endUser);
        if (!rmFields.contains("type")) converted.addProperty("type", fileInfo.type);
        if (!rmFields.contains("status")) converted.addProperty("status", fileInfo.status);
        if (converted.size() < 8 - rmFields.size())
            throw new IOException("there are no enough valid file info key in fields.");
        return converted.toString();
    }
}
