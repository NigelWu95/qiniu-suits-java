package com.qiniu.model.qdora;

import com.qiniu.util.JsonUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class PfopResultTest {

    @Test
    public void test() {
        String result = "{\"code\":200,\"items\":[{\"cmd\":\"avthumb/mp4\"}]}";
        PfopResult pfopResult = JsonUtils.fromJson(result, PfopResult.class);
        System.out.println(pfopResult.code);
        System.out.println(pfopResult.items.get(0).cmd);
    }

}