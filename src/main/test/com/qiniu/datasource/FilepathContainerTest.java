package com.qiniu.datasource;

import com.qiniu.interfaces.IReader;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class FilepathContainerTest {

    @Test
    public void testGetFileReaders() throws IOException {
        FilepathContainer filepathContainer = new FilepathContainer("Downloads", "tab", "\t",
                null, new HashMap<String, String>(){{ put("0", "path"); }},
                null, 10, 10);
        try {
            List<IReader<Iterator<String>>> readers = filepathContainer.getFileReaders("/Users/wubingheng/Downloads");
            for (IReader<Iterator<String>> reader : readers) {
                System.out.println(reader.readLines());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}