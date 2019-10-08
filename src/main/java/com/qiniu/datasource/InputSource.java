package com.qiniu.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.convert.LineToMap;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.util.FileUtils;
import com.qiniu.util.HttpRespUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class InputSource {

    private String parse;
    private String separator;
    private String addKeyPrefix;
    private String rmKeyPrefix;
    private Map<String, String> indexMap;
    private boolean isQupload;

    public InputSource(String parse, String separator, String addKeyPrefix, String rmKeyPrefix,
                       Map<String, String> indexMap) {
        this.parse = parse;
        this.separator = separator;
        this.addKeyPrefix = addKeyPrefix;
        this.rmKeyPrefix = rmKeyPrefix;
        this.indexMap = indexMap;
    }

    public void export(InputStream inputStream, ILineProcess<Map<String, String >> processor) throws IOException {
        ITypeConvert<String, Map<String, String>> converter = new LineToMap(parse, separator, addKeyPrefix,
                rmKeyPrefix, indexMap);
        if (processor != null) isQupload = "qupload".equals(processor.getProcessName());
        InputStreamReader reader = new InputStreamReader(inputStream);
        StringBuilder stringBuilder = new StringBuilder();
        int t;
        String line;
        Map<String, String> converted;
        boolean quit = false;
        while (!quit) {
            System.out.println("please input line data to process: ");
            while ((t = reader.read()) != -1) {
                if(t == '\n') break;
                stringBuilder.append((char)t);
            }
            line = stringBuilder.toString();
            stringBuilder.delete(0, stringBuilder.length());
            quit = line.isEmpty();
            if (!quit) {
                if (processor != null) {
                    converted = converter.convertToV(line);
                    if (isQupload) {
                        if (converted.containsKey("filepath")) {
                            converted.put("filepath", FileUtils.convertToRealPath(converted.get("filepath")));
                        } else {
                            converted.put("filepath", FileUtils.convertToRealPath(converted.get("key")));
                        }
                    }
                    try {
                        System.out.println(processor.processLine(converted));
                    } catch (QiniuException e) {
                        if (HttpRespUtils.checkException(e, 2) < -1) throw e;
                        else System.out.println(HttpRespUtils.getMessage(e));
                    }
                } else {
                    System.out.println(line);
                }
            }
        }
    }
}
