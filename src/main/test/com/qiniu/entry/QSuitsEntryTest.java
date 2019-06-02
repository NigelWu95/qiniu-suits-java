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
                "-ak=ksjadasfdljdhsjaksdfdjfgksjdsasdfsghfhfg",
                "-sk=adsjkfadsfdgfhgjjhfgdfdfsdgfhgfdsrtyhvgh"
                , "-domain=xxx.com"
//                , "-line=\"ashdfasd\""
                , "-key=\"0000021-result\""
//                , "-process=exportts"
//                , "-url=http://xxx.com/1618060907DD4B1FA72E50491C08B356.m3u8"
//                , "-process=privateurl"
//                , "-url=http://xxx.com/test.gif"
//                , "-process=avinfo"
//                , "-process=qhash"
//                , "-process=asyncfetch"
                , "-to-bucket=ts-work"
//                , "-url=http://p3l1d5mx4.bkt.clouddn.com/1080P/%E8%B6%85%E6%B8%85%E9%87%91%E6%B5%B7%E6%B9%96%E7%BE%8E%E6%99%AF.mp4"
//                , "-url-index=ghl"
//                , "-parse=json"
                , "-bucket=ts-work"
//                , "-process=delete"
                , "-process=stat"
//                , "-process=type"
//                , "-type=1"
//                , "-process=status"
//                , "-status=1"
//                , "-process=lifecycle"
//                , "-days=1"
//                , "-process=copy"
//                , "-process=move"
//                , "-process=rename"
//                , "-prefix-force=true"
//                , "-add-prefix=a/"
                , "-to-key=0000021-ret"
        };
        QSuitsEntry qSuitsEntry = new QSuitsEntry(ParamsUtils.toParamsMap(args));
        ILineProcess<Map<String, String>> processor;
        processor = qSuitsEntry.whichNextProcessor(true);
        if (processor == null) throw new IOException("no process defined.");
        CommonParams commonParams = qSuitsEntry.getCommonParams();
        System.out.println(processor.processLine(commonParams.getMapLine()));
    }
}