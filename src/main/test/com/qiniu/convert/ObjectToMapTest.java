package com.qiniu.convert;

import com.google.gson.JsonObject;
import com.qiniu.config.ParamsConfig;
import com.qiniu.config.PropertiesFile;
import com.qiniu.datasource.QiniuLister;
import com.qiniu.entry.QSuitsEntry;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.JsonUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ObjectToMapTest {

    private List<FileInfo> fileInfoList;
    private ITypeConvert<FileInfo, Map<String, String>> mapConverter;

//    @Before
//    public void init() throws Exception {
//        IEntryParam entryParam = new ParamsConfig(new PropertiesFile("resources/.application.properties").getProperties());
//        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam);
//        mapConverter = new QOSObjToMap(qSuitsEntry.getCommonParams().getIndexMap());
//        String accessKey = entryParam.getValue("ak");
//        String secretKey = entryParam.getValue("sk");
//        String bucket = entryParam.getValue("bucket");
//        QiniuLister qiniuLister = new QiniuLister(new BucketManager(Auth.create(accessKey, secretKey), new Configuration()), bucket,
//                null, null, null, 10000);
//        fileInfoList = qiniuLister.currents();
//    }
//
//    @Test
//    public void testConvertToVList() {
//        List<Map<String, String>> mapList = mapConverter.convertToVList(fileInfoList);
//        System.out.println(mapList);
//        System.out.println(mapConverter.errorSize());
//        System.out.println(mapConverter.errorLines());
//        System.out.println(mapConverter.errorSize());
//    }

    @Test
    public void testJsonConvert() {
        JsonObject jsonObject = new JsonObject();
        JsonObject object1 = new JsonObject();
        jsonObject.addProperty("a", "aaa");
        object1.addProperty("1", 111);
        object1.addProperty("b", "bbb");
        jsonObject.addProperty("2", 222);
        jsonObject.add("json", object1);
        System.out.println(jsonObject.toString());
//        System.out.println(jsonObject.getAsString());
        System.out.println(jsonObject.get("a"));
        System.out.println(jsonObject.get("2"));
        System.out.println(jsonObject.get("a").getAsString());
        System.out.println(jsonObject.get("json"));
        System.out.println(JsonUtils.toString(jsonObject.get("a")));
        System.out.println(JsonUtils.toString(jsonObject.get("2")));
        System.out.println(String.valueOf(jsonObject.get("json")));
        System.out.println(String.valueOf(jsonObject.get("2")));
        System.out.println(String.valueOf(jsonObject.get("a")));
    }
}