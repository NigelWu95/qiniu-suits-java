package com.qiniu.config;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ParamsConfigTest {

    private ParamsConfig paramsConfig;

    @Before
    public void init() throws IOException {
        paramsConfig = new ParamsConfig("resources" + System.getProperty("file.separator") + ".qiniu..properties");
    }

    @Test
    public void testGetValue() throws IOException {
        System.out.println(paramsConfig.getValue("no", "no"));
        System.out.println(paramsConfig.getValue("use-https", "true"));
        System.out.println(paramsConfig.getValue("use-https") == null);
        System.out.println(paramsConfig.getValue("use-https").equals(""));
        System.out.println(paramsConfig.getValue("no"));
    }
}
