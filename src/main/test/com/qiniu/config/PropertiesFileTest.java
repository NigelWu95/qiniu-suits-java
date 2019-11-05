package com.qiniu.config;

import org.junit.Test;

import java.io.IOException;

public class PropertiesFileTest {

    @Test
    public void testGetKeys() {
        try {
            PropertiesFile propertiesFile =
                    new PropertiesFile("application.properties");
//                    new PropertiesFile("resources/.application.properties");
            System.out.println(propertiesFile.getKeys());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}