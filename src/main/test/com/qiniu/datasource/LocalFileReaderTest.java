package com.qiniu.datasource;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class LocalFileReaderTest {

    @Test
    public void testInit() {
        try {
            LocalFileReader localFileReader = new LocalFileReader(new File("../ test/test.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
