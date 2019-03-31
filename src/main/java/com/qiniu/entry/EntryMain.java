package com.qiniu.entry;

import com.qiniu.datasource.IDataSource;
import com.qiniu.interfaces.ILineProcess;
import java.util.Map;

public class EntryMain {

    public static void main(String[] args) throws Exception {
        QSuitsEntry qSuitsEntry = new QSuitsEntry(args);
        ILineProcess<Map<String, String>> processor = qSuitsEntry.getProcessor();
        IDataSource dataSource = qSuitsEntry.getDataSource();
        if (dataSource != null) {
            dataSource.setProcessor(processor);
            dataSource.export();
        }
        if (processor != null) processor.closeResource();
    }
}
