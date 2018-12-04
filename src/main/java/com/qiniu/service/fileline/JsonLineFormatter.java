package com.qiniu.service.fileline;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.storage.model.FileInfo;

public class JsonLineFormatter implements IStringFormat {

    public String toFormatString(FileInfo fileInfo) {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(fileInfo).replace("\\\\", "\\");
    }
}
