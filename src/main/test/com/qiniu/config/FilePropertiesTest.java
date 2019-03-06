package com.qiniu.config;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class FilePropertiesTest {

    private FileProperties fileProperties;

    @Before
    public void init() throws IOException {
        fileProperties = new FileProperties("resources" + System.getProperty("file.separator") + ".qiniu.properties");
    }

    @Test
    public void testGetValue() throws IOException {
        System.out.println(fileProperties.getValue("no", "no"));
        System.out.println(fileProperties.getValue("use-https", "true"));
        System.out.println(fileProperties.getValue("use-https") == null);
        System.out.println(fileProperties.getValue("use-https").equals(""));
        System.out.println(fileProperties.getValue("no"));
    }
}