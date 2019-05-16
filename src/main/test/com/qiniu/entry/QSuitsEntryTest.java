package com.qiniu.entry;

import com.qiniu.config.ParamsConfig;
import com.qiniu.datasource.IDataSource;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.qoss.CopyFile;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class QSuitsEntryTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testEntry() throws Exception {
        IEntryParam entryParam = new ParamsConfig("resources/.application.properties");
        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam);
        ILineProcess<Map<String, String>> processor = qSuitsEntry.getProcessor();
        IDataSource dataSource = qSuitsEntry.getDataSource();
        if (dataSource != null) {
            dataSource.setProcessor(processor);
            dataSource.export();
        }
        if (processor != null) processor.closeResource();
    }
}