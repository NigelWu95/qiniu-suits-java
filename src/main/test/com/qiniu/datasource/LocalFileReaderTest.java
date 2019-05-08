package com.qiniu.datasource;

import org.junit.Test;

import java.io.IOException;

public class LocalFileReaderTest {

    @Test
    public void testInit() {
        try {
            LocalFileReader localFileReader = new LocalFileReader("../ test/test.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
