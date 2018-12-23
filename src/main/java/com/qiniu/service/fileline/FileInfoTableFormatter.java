package com.qiniu.service.fileline;

import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FileInfoTableFormatter implements IStringFormat<FileInfo> {

    private String separator;

    public FileInfoTableFormatter(String separator) {
        this.separator = separator;
    }

    public String toFormatString(FileInfo fileInfo, List<String> usedFields) throws IOException {
        StringBuilder converted = new StringBuilder();
        usedFields.forEach(key -> {
            switch (key) {
                case "key": converted.append(fileInfo.key); break;
                case "fsize": converted.append(fileInfo.fsize); break;
                case "putTime": converted.append(fileInfo.putTime); break;
                case "mimeType": converted.append(fileInfo.mimeType); break;
                case "endUser": converted.append(fileInfo.endUser); break;
                case "type": converted.append(fileInfo.type); break;
                case "status": converted.append(fileInfo.status); break;
            }
            converted.append(separator);
        });
        if (converted.toString().split(separator).length == 0)
            throw new IOException("there are no valid file info key in fields.");
        return converted.toString();
    }
}
