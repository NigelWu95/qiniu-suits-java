package com.qiniu.entry;

import com.qiniu.datasource.IDataSource;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import java.util.List;
import java.util.Map;

public class EntryMain {

    public static void main(String[] args) throws Exception {
        QSuitsEntry qSuitsEntry = new QSuitsEntry(args);
        IEntryParam entryParam = qSuitsEntry.getEntryParam();
        Configuration configuration = qSuitsEntry.getConfiguration();
        CommonParams commonParams = qSuitsEntry.getCommonParams();
        ILineProcess<Map<String, String>> processor = new ProcessorChoice(entryParam, configuration, commonParams).get();
        IDataSource dataSource = qSuitsEntry.getDataSource();
        // 这些参数需要在获取 processor 之后再访问，因为可能由于 ProcessorChoice 的过程对参数的默认值进行修改
        boolean saveTotal = commonParams.getSaveTotal();
        String saveFormat = commonParams.getSaveFormat();
        String saveSeparator = commonParams.getSaveSeparator();
        List<String> rmFields = commonParams.getRmFields();
        if (dataSource != null) {
            dataSource.setResultOptions(saveTotal, saveFormat, saveSeparator, rmFields);
            dataSource.setProcessor(processor);
            dataSource.export();
        }
        if (processor != null) processor.closeResource();
    }
}
