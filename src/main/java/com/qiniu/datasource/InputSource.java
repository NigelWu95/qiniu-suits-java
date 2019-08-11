package com.qiniu.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.convert.LineToMap;
import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.util.HttpRespUtils;
import com.qiniu.util.LogUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class InputSource {

    protected String parse;
    protected String separator;
    protected String addKeyPrefix;
    protected String rmKeyPrefix;
    protected Map<String, String> indexMap;

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
                    try {
                        System.out.println(processor.processLine(converted));
                    } catch (QiniuException e) {
                        if (HttpRespUtils.checkException(e, 2) < -1) throw e;
                        else System.out.println(LogUtils.getMessage(e));
                    }
                }
                else System.out.println(line);
            }
        }
    }
}
