package com.qiniu.entry;

import com.qiniu.model.parameter.FileInputParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputInfoParser {

    private List<String> needKeyProcesses = new ArrayList<String>(){{
        add("filter");
        add("asyncfetch");
        add("status");
        add("type");
        add("copy");
        add("move");
        add("rename");
        add("delete");
        add("stat");
        add("qhash");
        add("lifecycle");
        add("pfop");
        add("avinfo");
        add("privateurl");
    }};
    private List<String> needHashProcesses = new ArrayList<String>(){{
        add("filter");
        add("asyncfetch");
    }};
    private List<String> needFsizeProcesses = new ArrayList<String>(){{
        add("filter");
    }};
    private List<String> needPutTimeProcesses = new ArrayList<String>(){{
        add("filter");
    }};
    private List<String> needMimeTypeProcesses = new ArrayList<String>(){{
        add("filter");
    }};
    private List<String> needEndUserProcesses = new ArrayList<String>(){{
        add("filter");
    }};
    private List<String> needTypeProcesses = new ArrayList<String>(){{
        add("filter");
    }};
    private List<String> needStatusProcesses = new ArrayList<String>(){{
        add("filter");
    }};

    public Map<String, String> getInfoIndexMap(FileInputParams fileInputParams, String process) throws IOException {
        Map<String, String> infoIndexMap = new HashMap<>();
        return infoIndexMap;
    }
}
