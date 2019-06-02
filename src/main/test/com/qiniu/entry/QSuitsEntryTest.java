package com.qiniu.entry;

import com.qiniu.config.ParamsConfig;
import com.qiniu.datasource.IDataSource;
import com.qiniu.datasource.ScannerSource;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.qoss.CopyFile;
import com.qiniu.util.ParamsUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class QSuitsEntryTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testEntry1() throws Exception {
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

    @Test
    public void testEntry2() throws Exception {
        String[] args = new String[]{
                "-line=\"ashdfasd\""
                , "-process=privateurl"
                , "-ak=ksjadasfdljdhsjaksdfdjfgksjdsasdfsghfhfg"
                , "-sk=adsjkfadsfdgfhgjjhfgdfdfsdgfhgfdsrtyhvgh"
//                , "-url=http://test.qiniuts.com/test.gif"
                , "-domain=www.nigel.net.cn"
//                , "-url-index=ghl"
//                , "-parse=json"
        };
        QSuitsEntry qSuitsEntry = new QSuitsEntry(ParamsUtils.toParamsMap(args));
        ILineProcess<Map<String, String>> processor;
        processor = qSuitsEntry.whichNextProcessor(true);
        if (processor == null) throw new IOException("no process defined.");
        CommonParams commonParams = qSuitsEntry.getCommonParams();
        System.out.println(processor.processLine(commonParams.getMapLine()));
    }
}