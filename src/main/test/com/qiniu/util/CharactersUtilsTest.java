package com.qiniu.util;

import org.junit.Test;

import javax.xml.bind.DatatypeConverter;

public class CharactersUtilsTest {

    @Test
    public void testBytesToHexString() {
    }

    @Test
    public void testHexStringToBytes() {
        byte[] bytes = CharactersUtils.hexStringToBytes("0076f1dc03455a9431341999c4492dca");
        System.out.println(CharactersUtils.bytesToHexString(bytes));
        System.out.println(UrlSafeBase64.encodeToString(bytes));
        System.out.println(DatatypeConverter.printBase64Binary(bytes));
    }
}
