package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;
import java.util.*;

public class FileInputParams extends CommonParams {

    private String filePath;
    private String parseType;
    private String separator;
    private String urlIndex;
    private String md5Index;
    private String newKeyIndex;
    private String fopsIndex;
    private String pidIndex;
    private String avnfoIndex;
    private Map<String, String> indexMap = new HashMap<>();
    private List<String> needUrlIndex = new ArrayList<String>(){{
        add("asyncfetch");
        add("privateurl");
        add("qhash");
        add("avinfo");
    }};
    private List<String> needMd5Index = new ArrayList<String>(){{
        add("asyncfetch");
    }};
    private List<String> needNewKeyIndex = new ArrayList<String>(){{
        add("rename");
        add("copy");
    }};
    private List<String> needFopsIndex = new ArrayList<String>(){{
        add("pfop");
    }};
    private List<String> needPidIndex = new ArrayList<String>(){{
        add("pfopresult");
    }};
    private List<String> needAvinfoIndex = new ArrayList<String>(){{
        add("pfopcmd");
    }};

    public FileInputParams(IEntryParam entryParam) throws IOException {
        super(entryParam);
        filePath = entryParam.getValue("file-path", null);
        parseType = entryParam.getValue("parse-type", "table");
        if (!parseType.matches("(json|table)")) {
            throw new IOException("no incorrect parse type, please set it as \"json\" or \"table\".");
        }
        separator = entryParam.getValue("in-separator", "\t");

        String indexes = entryParam.getValue("indexes", "");
        List<String> keys = Arrays.asList("key", "hash", "fsize", "putTime", "mimeType", "type", "status", "endUser");
        if ("table".equals(getParseType())) {
            if ("".equals(indexes)) {
                indexMap.put("0", keys.get(0));
            } else if (indexes.matches("(\\d+,)*\\d")) {
                List<String> indexList = Arrays.asList(indexes.split(","));
                if (indexList.size() > 8) {
                    throw new IOException("the file info's index length is too long.");
                } else {
                    for (int i = 0; i < indexList.size(); i++) {
                        if (indexList.get(i).matches("\\d+") && Integer.valueOf(indexList.get(i)) > -1)
                            indexMap.put(indexList.get(i), keys.get(i));
                    }
                }
            } else {
                throw new IOException("the index pattern is not supported.");
            }
        } else {
            List<String> indexList = Arrays.asList(indexes.split(","));
            if (indexList.size() == 0) {
                indexMap.put("key", keys.get(0));
            } else if (indexList.size() > 8) {
                throw new IOException("the file info's index length is too long.");
            } else {
                for (int i = 0; i < indexList.size(); i++) { indexMap.put(indexList.get(i), keys.get(i)); }
            }
        }

        setUrlIndex(entryParam.getValue("url-index", null));
        setMd5Index(entryParam.getValue("md5-index", null));
        setNewKeyIndex(entryParam.getValue("newKey-index", null));
        setFopsIndex(entryParam.getValue("fops-index", null));
        setPidIndex(entryParam.getValue("persistentId-index", null));
        setAvnfoIndex(entryParam.getValue("avinfo-index", null));
    }

    private String getIndex(String index, String indexName) throws IOException {
        if (index == null || "".equals(index)) {
            throw new IOException("no incorrect " + indexName + "-index.");
        } else {
            if ("json".equals(getParseType())) {
                return index;
            } else if ("table".equals(getParseType())) {
                if (index.matches("\\d")) {
                    return index;
                } else {
                    throw new IOException("no incorrect " + indexName + "-index, it should be a number.");
                }
            } else {
                // 其他情况忽略该索引
                return "";
            }
        }
    }

    private void setUrlIndex(String urlIndex) throws IOException {
        this.urlIndex = getIndex(urlIndex, "url");
        if (needUrlIndex.contains(getProcess())) {
            if (!"".equals(this.urlIndex)) indexMap.put(this.urlIndex, this.urlIndex);
        }
    }

    private void setMd5Index(String md5Index) throws IOException {
        this.md5Index = getIndex(md5Index, "md5");
        if (needMd5Index.contains(getProcess())) {
            if (!"".equals(this.md5Index)) indexMap.put(this.md5Index, this.md5Index);
        }
    }

    private void setNewKeyIndex(String newKeyIndex) throws IOException {
        this.newKeyIndex = getIndex(newKeyIndex, "newKey");
        if (needNewKeyIndex.contains(getProcess())) {
            if (!"".equals(this.newKeyIndex)) indexMap.put(this.newKeyIndex, this.newKeyIndex);
            if (indexMap.size() < 2) throw new IOException("please check the key and newKey index, two index can" +
                    "not be same with each other.");
        }
    }

    private void setFopsIndex(String fopsIndex) throws IOException {
        this.fopsIndex = getIndex(fopsIndex, "fops");
        if (needFopsIndex.contains(getProcess())) {
            if (!"".equals(this.fopsIndex)) indexMap.put(this.fopsIndex, this.fopsIndex);
            if (indexMap.size() < 2) throw new IOException("please check the key and fops index, two index can" +
                    "not be same with each other.");
        }
    }

    private void setPidIndex(String pidIndex) throws IOException {
        this.pidIndex = getIndex(pidIndex, "persistentId");
        if (needPidIndex.contains(getProcess())) {
            if (!"".equals(this.pidIndex)) indexMap.put(this.pidIndex, this.pidIndex);
        }
    }

    private void setAvnfoIndex(String avnfoIndex) throws IOException {
        this.avnfoIndex = getIndex(avnfoIndex, "avinfo");
        if (needAvinfoIndex.contains(getProcess())) {
            if (!"".equals(this.avnfoIndex)) indexMap.put(this.avnfoIndex, this.avnfoIndex);
        }
    }

    public String getParseType() {
        return parseType;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getSeparator() {
        return separator;
    }

    public Map<String, String> getIndexMap() {
        return indexMap;
    }

    public String getUrlIndex() {
        return urlIndex;
    }

    public String getMd5Index() {
        return md5Index;
    }

    public String getNewKeyIndex() {
        return newKeyIndex;
    }

    public String getFopsIndex() {
        return fopsIndex;
    }

    public String getPersistentIdIndex() {
        return pidIndex;
    }

    public String getAvnfoIndex() {
        return avnfoIndex;
    }
}
