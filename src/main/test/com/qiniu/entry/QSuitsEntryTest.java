package com.qiniu.entry;

import com.qiniu.config.ParamsConfig;
import com.qiniu.datasource.IDataSource;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.process.qoss.CopyFile;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QSuitsEntryTest {

    @Test
    public void testEntry() throws Exception {
        IEntryParam entryParam = new ParamsConfig("resources/.qiniu.properties");
        Map<String, String> paramsMap = entryParam.getParamsMap();
        String savePath = paramsMap.get("save-path");
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

        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam);;
        CommonParams commonParams = qSuitsEntry.getCommonParams();
        CopyFile processor = (CopyFile) qSuitsEntry.getProcessor();
        IDataSource dataSource = qSuitsEntry.getDataSource();
        boolean saveTotal = commonParams.getSaveTotal();
        String saveFormat = commonParams.getSaveFormat();
        String saveSeparator = commonParams.getSaveSeparator();
        List<String> rmFields = commonParams.getRmFields();

        // 不断去更改 bucket 做执行
        for (String bucket : buckets) {
            commonParams.setBucket(bucket + "-src");
            commonParams.setSavePath(savePath + "/" + bucket);
            if (processor != null) {
                processor.updateCopy(bucket + "-src", bucket, null, null, null);
                processor.updateSavePath(savePath + "/" + bucket);
            }
            if (dataSource != null) {
                dataSource.setSaveOptions(saveTotal, saveFormat, saveSeparator, rmFields);
                dataSource.updateSettings(commonParams);
                dataSource.setProcessor(processor);
                dataSource.export();
            }
        }

        if (processor != null) processor.closeResource();
    }
}