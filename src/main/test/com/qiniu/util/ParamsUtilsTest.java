package com.qiniu.util;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

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
}