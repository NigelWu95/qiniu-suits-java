package com.qiniu.custom.miaop;

import com.qiniu.common.QiniuException;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.storage.model.FileInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class MirrorSrcHashTest {

    private MirrorSrcHash mirrorSrcHash;

    @Before
    public void init() {
        this.mirrorSrcHash = new MirrorSrcHash("miaopai-s.oss-cn-beijing.aliyuncs.com", "../miaopai-test");
    }

    @Test
    public void testSingleWithRetry() throws QiniuException {
        Map<String,String> line = new HashMap<String, String>(){{ put("0", "images%2F6469036258323595265_audit_9.jpg"); }};
        String md5 = mirrorSrcHash.singleWithRetry(line.get("0"), 3);
        System.out.println(md5);
    }
}