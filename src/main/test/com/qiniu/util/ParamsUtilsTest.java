package com.qiniu.util;

import org.junit.Test;
import sun.misc.BASE64Decoder;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

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
        BASE64Decoder decoder = new BASE64Decoder();
        Map<String, String> map = ParamsUtils.toParamsMap(FileUtils.realPathWithUserHome("~" + FileUtils.pathSeparator + ".qsuits.account"));
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String account = entry.getKey().split("-")[0];
            System.out.print(entry.getKey() + "\t" + entry.getValue());
            System.out.println("\t" + new String(decoder.decodeBuffer(entry.getValue().substring(8))));
        }
    }
}