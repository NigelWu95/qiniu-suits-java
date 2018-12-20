package com.qiniu.entry;

import com.qiniu.common.QiniuException;
import com.qiniu.model.parameter.FileInputParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputInfoParser {

    private List<String> needKeyProcesses = new ArrayList<String>(){{
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
        add("filter");
    }};
    private List<String> needHashProcesses = new ArrayList<String>(){{
        add("asyncfetch");
        add("filter");
    }};
    private List<String> needFsizeProcesses = new ArrayList<String>(){{
        add("filter");
    }};
    private List<String> needPutTimeProcesses = new ArrayList<String>(){{
        add("filter");
    }};
    private List<String> needMimeTypeProcesses = new ArrayList<String>(){{
        add("asyncfetch");
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
    private List<String> needMd5Processes = new ArrayList<String>(){{
        add("asyncfetch");
        add("filter");
    }};
    private List<String> needFopsProcesses = new ArrayList<String>(){{
        add("pfop");
        add("filter");
    }};
    private List<String> needPersistentIdProcesses = new ArrayList<String>(){{
        add("pfopresult");
    }};
    private List<String> needNewKeyProcesses = new ArrayList<String>(){{
        add("rename");
    }};

    public Map<String, String> getInfoIndexMap(FileInputParams fileInputParams) throws QiniuException {
        Map<String, String> infoIndexMap = new HashMap<>();
        String process = fileInputParams.getProcess();
        if (needKeyProcesses.contains(process)) infoIndexMap.put(fileInputParams.getKeyIndex(), "key");
        if (needHashProcesses.contains(process)) infoIndexMap.put(fileInputParams.getHashIndex(), "hash");
        if (needFsizeProcesses.contains(process)) infoIndexMap.put(fileInputParams.getFsizeIndex(), "fsize");
        if (needPutTimeProcesses.contains(process)) infoIndexMap.put(fileInputParams.getPutTimeIndex(), "putTime");
        if (needMimeTypeProcesses.contains(process)) infoIndexMap.put(fileInputParams.getMimeTypeIndex(), "mimeType");
        if (needEndUserProcesses.contains(process)) infoIndexMap.put(fileInputParams.getEndUserIndex(), "endUser");
        if (needTypeProcesses.contains(process)) infoIndexMap.put(fileInputParams.getTypeIndex(), "type");
        if (needStatusProcesses.contains(process)) infoIndexMap.put(fileInputParams.getStatusIndex(), "status");
        if (needMd5Processes.contains(process)) infoIndexMap.put(fileInputParams.getMd5Index(), "md5");
        if (needFopsProcesses.contains(process)) infoIndexMap.put(fileInputParams.getFopsIndex(), "fops");
        if (needPersistentIdProcesses.contains(process)) infoIndexMap.put(fileInputParams.getPersistentIdIndex(), "persistentId");
        if (needNewKeyProcesses.contains(process)) infoIndexMap.put(fileInputParams.getTargetKeyIndex(), "newKey");
        return infoIndexMap;
    }
}
