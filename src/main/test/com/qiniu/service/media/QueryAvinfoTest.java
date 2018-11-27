package com.qiniu.service.media;

import com.qiniu.common.QiniuException;
import com.qiniu.model.parameter.AvinfoParams;
import com.qiniu.storage.model.FileInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class QueryAvinfoTest {

    private QueryAvinfo queryAvinfo;

    @Before
    public void init() throws Exception {
        AvinfoParams avinfoParams = new AvinfoParams("resources/.qiniu.properties");
        this.queryAvinfo = new QueryAvinfo(avinfoParams.getDomain(), "../result", 0);
    }

    @Test
    public void testProcessFile() throws QiniuException {
        List<FileInfo> list = new ArrayList<>();
        FileInfo fileInfo1 = new FileInfo();
        FileInfo fileInfo2 = new FileInfo();
        FileInfo fileInfo3 = new FileInfo();
        fileInfo1.key = "video/335991/403191/1523488434903607_new.mp4";
        fileInfo2.key = "video/335991/403191/1523488480906009_new.mp4";
        fileInfo3.key = "video/335991/403191/1523488528908834_new.mp4";
        list.add(fileInfo1);
        list.add(fileInfo2);
        list.add(fileInfo3);
        queryAvinfo.processLine(list);
        queryAvinfo.closeResource();
    }
}