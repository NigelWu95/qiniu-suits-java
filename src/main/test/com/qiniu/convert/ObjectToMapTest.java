package com.qiniu.convert;

import com.qiniu.config.ParamsConfig;
import com.qiniu.datasource.QiniuLister;
import com.qiniu.entry.QSuitsEntry;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ObjectToMapTest {

    private List<FileInfo> fileInfoList;
    private ITypeConvert<FileInfo, Map<String, String>> mapConverter;

    @Before
    public void init() throws Exception {
        IEntryParam entryParam = new ParamsConfig("resources" + System.getProperty("file.separator") + ".qiniu.properties");
        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam);
        mapConverter = new QOSObjToMap(qSuitsEntry.getCommonParams().getIndexMap());
        String accessKey = entryParam.getValue("ak");
        String secretKey = entryParam.getValue("sk");
        String bucket = entryParam.getValue("bucket");
        QiniuLister qiniuLister = new QiniuLister(new BucketManager(Auth.create(accessKey, secretKey), new Configuration()), bucket,
                null, null, null, null, 10000);
        fileInfoList = qiniuLister.currents();
    }

    @Test
    public void testConvertToVList() {
        List<Map<String, String>> mapList = mapConverter.convertToVList(fileInfoList);
        System.out.println(mapConverter.errorSize());
        System.out.println(mapList);
        System.out.println(mapConverter.consumeErrors());
        System.out.println(mapConverter.errorSize());
    }
}