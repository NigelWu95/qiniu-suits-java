package com.qiniu.util;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ParamsUtilsTest {

    @Test
    public void testEscapeSplit() {
        String paramLine = "fragments,abc,\\,a";
        String[] escapes = new String[]{","};
        List<String> strings = ParamsUtils.escapeSplit(paramLine, ',', escapes);
        System.out.println(strings);
        paramLine += "\\\\b";
        escapes = new String[]{",", "\\"};
        strings = ParamsUtils.escapeSplit(paramLine, ',', escapes);
        System.out.println(strings);
        paramLine += "\\:";
        escapes = new String[]{",", "\\", ":"};
        strings = ParamsUtils.escapeSplit(paramLine, ',', escapes);
        System.out.println(strings);
        paramLine += "\\=";
        escapes = new String[]{",", "\\", ":", "="};
        strings = ParamsUtils.escapeSplit(paramLine, ',', escapes);
        System.out.println(strings);
    }
}