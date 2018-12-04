package com.qiniu.service.fileline;

import com.qiniu.model.qoss.Qhash;
import com.qiniu.service.interfaces.IStringFormat;
import com.qiniu.storage.model.FileInfo;

import java.util.Map;

public class QhashTableFormatter implements IStringFormat<Qhash> {

    private String separator;

    public QhashTableFormatter(String separator) {
        this.separator = separator;
    }

    public String toFormatString(Qhash qhash, Map<String, Boolean> variablesIfUse) {

        StringBuilder converted = new StringBuilder();
        variablesIfUse.forEach((key, value) -> {
            if (value) {
                try {
                    converted.append(String.valueOf(qhash.getClass().getField(key)));
                    converted.append(separator);
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                }
//                switch (key) {
//                    case "key": converted.append(qhash.hash); break;
//                    case "fsize": converted.append(String.valueOf(qhash.fsize)); break;
//                }
//                converted.append(separator);
            }
        });
        return converted.toString();
    }
}
