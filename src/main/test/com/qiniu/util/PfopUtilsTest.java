package com.qiniu.util;

import com.google.gson.JsonObject;
import org.junit.Test;

import static org.junit.Assert.*;

public class PfopUtilsTest {

    @Test
    public void testGenerateFopCmd() {
        JsonObject pfopJson = new JsonObject();
        pfopJson.addProperty("cmd", "avthumb/mp4");
        pfopJson.addProperty("saveas", "temp:$(name)");
        System.out.println(decodeFop(PfopUtils.generateFopCmd("000", pfopJson)));

        pfopJson.addProperty("saveas", "temp:.json-$(name)");
        System.out.println(decodeFop(PfopUtils.generateFopCmd("000", pfopJson)));

        pfopJson.addProperty("saveas", "temp:.json-$(name).json");
        System.out.println(decodeFop(PfopUtils.generateFopCmd("000", pfopJson)));

        pfopJson.addProperty("saveas", "temp:.json-$(name).json");
        System.out.println(decodeFop(PfopUtils.generateFopCmd("000.txt", pfopJson)));

        pfopJson.addProperty("saveas", "temp:$(name).json");
        System.out.println(decodeFop(PfopUtils.generateFopCmd("000", pfopJson)));

        pfopJson.addProperty("saveas", "temp:$(name).json");
        System.out.println(decodeFop(PfopUtils.generateFopCmd("000.txt", pfopJson)));

        pfopJson.addProperty("saveas", "temp:$(name)-.json");
        System.out.println(decodeFop(PfopUtils.generateFopCmd("000", pfopJson)));

        pfopJson.addProperty("saveas", "temp:.json-$(key).json");
        System.out.println(decodeFop(PfopUtils.generateFopCmd("000", pfopJson)));

        pfopJson.addProperty("saveas", "temp:.json-$(key).json");
        System.out.println(decodeFop(PfopUtils.generateFopCmd("000.txt", pfopJson)));

        pfopJson.addProperty("saveas", "temp:$(key).json");
        System.out.println(decodeFop(PfopUtils.generateFopCmd("000", pfopJson)));

        pfopJson.addProperty("saveas", "temp:$(key).json");
        System.out.println(decodeFop(PfopUtils.generateFopCmd("000.txt", pfopJson)));

        pfopJson.addProperty("saveas", "temp:$(key)-.json");
        System.out.println(decodeFop(PfopUtils.generateFopCmd("000", pfopJson)));
    }

    public static String decodeFop(String fop) {
        String[] items = fop.split("saveas/");
        return new String(UrlSafeBase64.decode(items[1].split("\\|")[0]));
    }
}