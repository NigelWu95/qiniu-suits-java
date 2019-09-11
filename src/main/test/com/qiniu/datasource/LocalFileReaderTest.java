package com.qiniu.datasource;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LocalFileReaderTest {

    @Test
    public void testInit() {
        try {
            LocalFileReader localFileReader = new LocalFileReader(new File("../ test/test.txt"), null, 10);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<String> list1 = new ArrayList<>();
        list1.add("1");
        list1.add("2");
        list1.add("3");
        List<String> list2 = new ArrayList<>(list1);
        list1.clear();
        System.out.println(list2);
    }
}
