package com.qiniu.service.media;

import com.qiniu.common.QiniuException;
import com.qiniu.model.parameter.AvinfoParams;
import com.qiniu.storage.model.FileInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryAvinfoTest {

    private QueryAvinfo queryAvinfo;

    @Before
    public void init() throws Exception {
        AvinfoParams avinfoParams = new AvinfoParams("resources/.qiniu.properties");
        this.queryAvinfo = new QueryAvinfo(avinfoParams.getDomain(), "../result", 0);
    }

    @Test
    public void testProcessFile() throws QiniuException {
        List<Map<String, String>> lineList = new ArrayList<>();
        lineList.add(new HashMap<String, String>(){{ put("0", "video/335991/403191/1523488434903607_new.mp4"); }});
        lineList.add(new HashMap<String, String>(){{ put("0", "video/335991/403191/1523488480906009_new.mp4"); }});
        lineList.add(new HashMap<String, String>(){{ put("0", "video/335991/403191/1523488528908834_new.mp4"); }});
        queryAvinfo.processLine(lineList);
        queryAvinfo.closeResource();
    }
}