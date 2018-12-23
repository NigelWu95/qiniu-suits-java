package com.qiniu.service.fileline;

import com.google.gson.JsonObject;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.List;

public class FileInfoJsonFormatter implements IStringFormat<FileInfo> {

    public String toFormatString(FileInfo fileInfo, List<String> usedFields) throws IOException {
        JsonObject converted = new JsonObject();
        usedFields.forEach(key-> {
            switch (key) {
                case "key": converted.addProperty(key, fileInfo.key); break;
                case "fsize": converted.addProperty(key, fileInfo.fsize); break;
                case "putTime": converted.addProperty(key, fileInfo.putTime); break;
                case "mimeType": converted.addProperty(key, fileInfo.mimeType); break;
                case "endUser": converted.addProperty(key, fileInfo.endUser); break;
                case "type": converted.addProperty(key, fileInfo.type); break;
                case "status": converted.addProperty(key, fileInfo.status); break;
            }
        });
        if (converted.size() == 0) throw new IOException("there are no valid file info key in fields.");
        return converted.toString();
    }
}
