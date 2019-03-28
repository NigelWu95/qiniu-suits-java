package com.qiniu.entry;

import com.qiniu.config.ParamsConfig;
import com.qiniu.datasource.IDataSource;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QSuitsEntryTest {

    @Test
    public void testEntry() throws Exception {
        IEntryParam entryParam = new ParamsConfig("resources/.qiniu.properties");
        Map<String, String> paramsMap = entryParam.getParamsMap();
        List<String> buckets = new ArrayList<String>(){{
            add("fhyfhy261");
            add("fhyfhy262");
            add("fhyfhy263");
            add("fhyfhy264");
            add("fhyfhy265");
            add("fhyfhy266");
            add("fhyfhy267");
            add("fhyfhy268");
            add("fhyfhy269");
            add("fhyfhy270");
            add("fhyfhy271");
            add("fhyfhy272");
            add("fhyfhy273");
        }};

        QSuitsEntry qSuitsEntry;
        Configuration configuration;
        CommonParams commonParams;
        ILineProcess<Map<String, String>> processor;
        IDataSource dataSource;
        // 这些参数需要在获取 processor 之后再访问，因为可能由于 ProcessorChoice 的过程对参数的默认值进行修改
        boolean saveTotal;
        String saveFormat;
        String saveSeparator;
        List<String> rmFields;

        // 不断去更改 bucket 做执行
        for (String bucket : buckets) {
            paramsMap.put("bucket", bucket + "-src");
            paramsMap.put("to-bucket", bucket);
            paramsMap.put("save-path", paramsMap.get("save-path") + "/" + bucket);
            entryParam = new ParamsConfig(paramsMap);
            qSuitsEntry = new QSuitsEntry(entryParam);
            configuration = qSuitsEntry.getConfiguration();
            commonParams = qSuitsEntry.getCommonParams();
            processor = new ProcessorChoice(entryParam, configuration, commonParams).get();
            dataSource = qSuitsEntry.getDataSource();
            saveTotal = commonParams.getSaveTotal();
            saveFormat = commonParams.getSaveFormat();
            saveSeparator = commonParams.getSaveSeparator();
            rmFields = commonParams.getRmFields();
            if (dataSource != null) {
                dataSource.setResultOptions(saveTotal, saveFormat, saveSeparator, rmFields);
                dataSource.setProcessor(processor);
                dataSource.export();
            }
            if (processor != null) processor.closeResource();
        }
    }
}