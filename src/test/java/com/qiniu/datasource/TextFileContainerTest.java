package com.qiniu.datasource;

import com.qiniu.util.FileUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TextFileContainerTest {

    @Test
    public void getFileReaders() throws Exception {

        String filePath = "../../Github/temp";
        String parse = "tab";
        String separator = ",";
        Map<String, Map<String, String>> linesMap = null;
        Map<String, String> indexMap = new HashMap<String, String>(){{
            put("key", "0");
        }};
        TextFileContainer textFileContainer = new TextFileContainer(filePath, parse, separator, null, null,
                false, null, null, indexMap, new ArrayList<String>(){{ add("key");}}, 100, 10);
        textFileContainer.export();
    }

    @Test
    public void test() {
        String type = FileUtils.contentType("../../Github/yppphoto");
        System.out.println(type);
    }
}