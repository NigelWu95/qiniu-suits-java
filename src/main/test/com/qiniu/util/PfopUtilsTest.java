package com.qiniu.util;

import com.google.gson.JsonObject;
import org.junit.Test;

import static org.junit.Assert.*;

public class PfopUtilsTest {

    @Test
    public void testGenerateFopCmd() {
        JsonObject pfopJson = new JsonObject();
        pfopJson.addProperty("cmd", "avthumb/mp4");
        pfopJson.addProperty("saveas", "temp:$(key)");
        String saveName = PfopUtils.generateFopCmd("000", pfopJson);
        System.out.println(saveName);
    }
}