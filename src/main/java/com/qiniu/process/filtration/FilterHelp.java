package com.qiniu.process.filtration;

import com.qiniu.config.JsonFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterHelp {

    public static List<String> checkList = new ArrayList<String>(){{ add("ext-mime"); }};
    public static JsonFile defaultCheckJson;

    public static void loadCheckJson() throws IOException {
        defaultCheckJson = new JsonFile("check.json");
    }
}
