package com.qiniu.config;

import com.google.gson.JsonObject;
import org.junit.Test;

import java.io.IOException;

public class JsonFileTest {

    @Test
    public void testGetKeys() {
        try {
            JsonFile jsonFile = new JsonFile("resources/lines.json");
            System.out.println(jsonFile.getKeys());
            System.out.println(jsonFile.getElement(""));
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonObject jsonObject = new JsonObject();
        System.out.println(jsonObject.get("test"));
        System.out.println(jsonObject.get("test") == null);
        jsonObject.add("test", null);
        System.out.println(jsonObject.toString());
    }
}