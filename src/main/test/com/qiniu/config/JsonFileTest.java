package com.qiniu.config;

import com.google.gson.JsonObject;
import org.junit.Test;

import java.io.IOException;

public class JsonFileTest {

    @Test
    public void testGetKeys() {
        try {
            JsonFile jsonFile = new JsonFile("check.json");
            System.out.println(jsonFile.getKeys());
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonObject jsonObject = new JsonObject();
        System.out.println(jsonObject.get("test"));
        System.out.println(jsonObject.get("test") == null);
    }
}