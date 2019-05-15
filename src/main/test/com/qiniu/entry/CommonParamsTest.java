package com.qiniu.entry;

import com.qiniu.config.ParamsConfig;
import com.qiniu.interfaces.IEntryParam;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

public class CommonParamsTest {

    @Test
    public void test() throws Exception {
        String[] args = new String[]{"-source=local"
//                ,"-source="
//                , "-indexes=[key:key,1,2,time:time]"
                , "-indexes=[key:key,1,2,time:time,a:\\:b]"
//                , "-indexes=[key:key,1,2,time:time,a:\\:b:c]"
                ,"-f-prefix=fragments,abc,\\,a"
//                ,"-f-mime=video"
                };
        IEntryParam entryParam = new ParamsConfig(args);
        CommonParams commonParams = new CommonParams(entryParam);
        Map<String,String> map = commonParams.getIndexMap();
        System.out.println(map);
    }
}