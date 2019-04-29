package com.qiniu.config;

import org.junit.Test;

import java.io.IOException;

public class ParamsConfigTest {

    private ParamsConfig paramsConfig;

    @Test
    public void testGetValueByCommandArgs() throws IOException {
        String[] args = new String[]{"list", "-a=\"\"", "-ak=\"1\"", "-sk=1", "-bucket=1", "-multi=1", "-max-threads=1", "-f-regex=9xiu/\\d*\\.jpg"};
        paramsConfig = new ParamsConfig(args);
        System.out.println(paramsConfig.getValue("ak"));
        System.out.println(paramsConfig.getValue("ab", "ab"));
        System.out.println(paramsConfig.getValue("f-regex", "ab"));
    }

    @Test
    public void testGetValueByFileProperties() throws IOException {
        paramsConfig = new ParamsConfig("resources" + System.getProperty("file.separator") + ".qiniu..properties");
        System.out.println(paramsConfig.getValue("no", "no"));
        System.out.println(paramsConfig.getValue("use-https", "true"));
        System.out.println(paramsConfig.getValue("use-https") == null);
        System.out.println(paramsConfig.getValue("use-https").equals(""));
        System.out.println(paramsConfig.getValue("no"));
    }
}
