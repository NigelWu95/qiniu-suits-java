package com.qiniu.service.fileline;

import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.storage.model.FileInfo;

import java.util.Map;

public class TableLineFormatter implements IStringFormat {

    private String separator;

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String toFormatString(FileInfo fileInfo, Map<String, Boolean> variablesIfUse) {

        StringBuilder converted = new StringBuilder();
        variablesIfUse.forEach((key, value) -> {
            if (value) {
                switch (key) {
                    case "key": converted.append(fileInfo.key); break;
                    case "fsize": converted.append(String.valueOf(fileInfo.fsize)); break;
                    case "putTime": converted.append(String.valueOf(fileInfo.putTime)); break;
                    case "mimeType": converted.append(fileInfo.mimeType); break;
                    case "endUser": converted.append(fileInfo.endUser); break;
                    case "type": converted.append(String.valueOf(fileInfo.type)); break;
//                    case "status": converted.append(key, fileInfo.status); break;
                }
                converted.append(separator);
            }
        });
        return converted.toString();
    }
}
