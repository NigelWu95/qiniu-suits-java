package com.qiniu.util;

import com.qiniu.config.PropertiesFile;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class ParamsUtilsTest {

    @Test
    public void testEscapeSplit() throws IOException {
        String paramLine;
        paramLine = ",\\,a,b,,c,\\,a";
        paramLine = "\\,a,b,,c,\\,a";
        paramLine = "\\,a,b,\\,c,\\,a";
        String[] escapes = new String[]{","};
        String[] strings = ParamsUtils.escapeSplit(paramLine, ',', escapes, true);
        System.out.println(String.join("---", strings));
        paramLine += "\\\\b";
        escapes = new String[]{",", "\\"};
        strings = ParamsUtils.escapeSplit(paramLine, ',', escapes, true);
        System.out.println(String.join("---", strings));
        paramLine += "\\:";
        escapes = new String[]{",", "\\", ":"};
        strings = ParamsUtils.escapeSplit(paramLine, ',', escapes, true);
        System.out.println(String.join("---", strings));
        paramLine += "\\=";
        escapes = new String[]{",", "\\", ":", "="};
        strings = ParamsUtils.escapeSplit(paramLine, ',', escapes, true);
        System.out.println(String.join("---", strings));
    }

    @Test
    public void testAccountConfig() throws Exception {
        java.util.Base64.Decoder decoder = java.util.Base64.getDecoder();
        String accountFile = FileUtils.convertToRealPath("~" + FileUtils.pathSeparator + ".qsuits.account");
        Map<String, String> map = ParamsUtils.toParamsMap(accountFile);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().endsWith("id") || entry.getKey().endsWith("secret")) {
                System.out.println(entry.getKey() + "\t" + new String(decoder.decode((entry.getValue().substring(8)))));
            }
        }
        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        java.util.Base64.Encoder encoder = java.util.Base64.getEncoder();
        BufferedWriter writer = new BufferedWriter(new FileWriter(accountFile, true));
        writer.write("wbh2-qiniu-id=" + EncryptUtils.getRandomString(8) +
                new String(encoder.encode(accessKey.getBytes())));
        writer.newLine();
        writer.write("wbh2-qiniu-secret=" + EncryptUtils.getRandomString(8) +
                new String(encoder.encode(secretKey.getBytes())));
        writer.newLine();
        writer.close();
    }

    @Test
    public void testConfigParams() {
        try {
            Map<String, String> fileConfig = ParamsUtils.toParamsMap("resources/.application.properties");
            System.out.println(fileConfig);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}