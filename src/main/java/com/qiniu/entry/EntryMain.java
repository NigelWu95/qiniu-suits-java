package com.qiniu.entry;

import com.qiniu.datasource.IDataSource;
import com.qiniu.interfaces.ILineProcess;
import java.util.Map;

public class EntryMain {

    public static void main(String[] args) throws Exception {
        QSuitsEntry qSuitsEntry = new QSuitsEntry(args);
        CommonParams commonParams = qSuitsEntry.getCommonParams();
        ILineProcess<Map<String, String>> processor = qSuitsEntry.getProcessor();
        IDataSource dataSource = qSuitsEntry.getDataSource();
        if (dataSource != null) {
            // 如果设置了 filter，默认情况下不保留原始数据
            if (processor != null && "filter".equals(processor.getProcessName())) {
                commonParams.setSaveTotal(false);
                dataSource.updateSettings(commonParams);
            }
            dataSource.setProcessor(processor);
            dataSource.export();
        }
        if (processor != null) processor.closeResource();
    }
}
