package com.qiniu.datasource;

import com.qiniu.common.QiniuException;
import com.qiniu.entry.CommonParams;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.IResultOutput;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public abstract class ScannerSource<W, T> implements IDataSource<Scanner, IResultOutput<W>, T> {

    protected String parseFormat;
    protected String separator;
    protected String addKeyPrefix;
    protected String rmKeyPrefix;
    protected Map<String, String> indexMap;
    protected int unitLen;
    protected String savePath;
    protected boolean saveTotal;
    protected String saveFormat;
    protected String saveSeparator;
    protected List<String> rmFields;

    public ScannerSource(String parseFormat, String separator, String addKeyPrefix, String rmKeyPrefix,
                          Map<String, String> indexMap, int unitLen) {
        this.parseFormat = parseFormat;
        this.separator = separator;
        this.addKeyPrefix = addKeyPrefix;
        this.rmKeyPrefix = rmKeyPrefix;
        this.indexMap = indexMap;
        this.unitLen = unitLen;
        this.saveTotal = false; // 默认全记录不保存
    }

    // 不调用则各参数使用默认值
    public void setSaveOptions(String savePath, boolean saveTotal, String format, String separator, List<String> rmFields) {
        this.savePath = savePath;
        this.saveTotal = saveTotal;
        this.saveFormat = format;
        this.saveSeparator = separator;
        this.rmFields = rmFields;
    }

    // 通过 commonParams 来更新基本参数
    public void updateSettings(CommonParams commonParams) {
        this.parseFormat = commonParams.getParse();
        this.separator = commonParams.getSeparator();
        this.addKeyPrefix = commonParams.getAddKeyPrefix();
        this.rmKeyPrefix = commonParams.getRmKeyPrefix();
        this.indexMap = commonParams.getIndexMap();
        this.unitLen = commonParams.getUnitLen();
        this.savePath = commonParams.getSavePath();
        this.saveTotal = commonParams.getSaveTotal();
        this.saveFormat = commonParams.getSaveFormat();
        this.saveSeparator = commonParams.getSaveSeparator();
        this.rmFields = commonParams.getRmFields();
    }

    protected abstract ITypeConvert<String, T> getNewConverter() throws IOException;

    protected abstract ITypeConvert<T, String> getNewStringConverter() throws IOException;

    public void export(Scanner scanner, IResultOutput<W> saver, ILineProcess<T> processor) throws IOException {
        ITypeConvert<String, T> converter = getNewConverter();
        ITypeConvert<T, String> writeTypeConverter = getNewStringConverter();
        List<String> srcList = new ArrayList<>();
        List<T> convertedList;
        List<String> writeList;
        String line;
        boolean quit = false;
        int retry;
        while (!quit) { // scanner.hasNext() 不能读取空行，不能勇于判断
            line = scanner.nextLine();
            quit = line == null || line.isEmpty();
            if (!quit) srcList.add(line);
            if (srcList.size() >= unitLen || (quit && srcList.size() > 0)) {
                convertedList = converter.convertToVList(srcList);
                if (converter.errorSize() > 0)
                    saver.writeError(String.join("\n", converter.consumeErrors()), false);
                if (saveTotal) {
                    writeList = writeTypeConverter.convertToVList(convertedList);
                    if (writeList.size() > 0) saver.writeSuccess(String.join("\n", writeList), false);
                    if (writeTypeConverter.errorSize() > 0)
                        saver.writeError(String.join("\n", writeTypeConverter.consumeErrors()), false);
                }
                // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
                try {
                    if (processor != null) processor.processLine(convertedList);
                } catch (QiniuException e) {
                    // 这里其实逻辑上没有做重试次数的限制，因为返回的 retry 始终大于等于 -1，所以不是必须抛出的异常则会跳过，process 本身会
                    // 保存失败的记录，除非是 process 出现 599 状态码才会抛出异常
                    retry = HttpResponseUtils.checkException(e, 1);
                    if (retry == -2) throw e;
                }
                srcList.clear();
            }
        }
    }
}
