package com.qiniu.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.convert.LineToMap;
import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.util.HttpRespUtils;
import com.qiniu.util.LogUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

public class ScannerSource {

    protected String parse;
    protected String separator;
    protected String addKeyPrefix;
    protected String rmKeyPrefix;
    protected Map<String, String> indexMap;

    public ScannerSource(String parse, String separator, String addKeyPrefix, String rmKeyPrefix,
                          Map<String, String> indexMap) {
        this.parse = parse;
        this.separator = separator;
        this.addKeyPrefix = addKeyPrefix;
        this.rmKeyPrefix = rmKeyPrefix;
        this.indexMap = indexMap;
    }

    // 通过 commonParams 来更新基本参数
    public void updateSettings(CommonParams commonParams) {
        this.parse = commonParams.getParse();
        this.separator = commonParams.getSeparator();
        this.addKeyPrefix = commonParams.getAddKeyPrefix();
        this.rmKeyPrefix = commonParams.getRmKeyPrefix();
        this.indexMap = commonParams.getIndexMap();
    }

    public void export(Scanner scanner, ILineProcess<Map<String, String >> processor) throws IOException {
        ITypeConvert<String, Map<String, String>> converter = new LineToMap(parse, separator, addKeyPrefix,
                rmKeyPrefix, indexMap);
        String line;
        Map<String, String> converted;
        boolean quit = false;
        int retry;
        while (!quit) { // scanner.hasNext() 不能读取空行，不能勇于判断
            line = scanner.nextLine();
            quit = line == null || line.isEmpty();
            if (!quit) {
                if (processor != null) {
                    converted = converter.convertToV(line);
                    try {
                        System.out.println(processor.processLine(converted));
                    } catch (QiniuException e) {
                        retry = HttpRespUtils.checkException(e, 1);
                        if (retry == -2) throw e;
                        else System.out.println(LogUtils.getMessage(e));
                    }
                }
                else System.out.println(line);
            }
        }
    }
}
