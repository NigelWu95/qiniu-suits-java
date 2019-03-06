package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;
import java.util.*;

public class FileInputParams extends CommonParams {

    private String parseType;
    private String separator;
    private String indexes;
    private String urlIndex;
    private String md5Index;
    private String newKeyIndex;
    private String fopsIndex;
    private String pidIndex;
    private String avnfoIndex;
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
        parseType = entryParam.getValue("parse-type", null);
        separator = entryParam.getValue("in-separator", "\t");
        indexes = entryParam.getValue("indexes", null);
        urlIndex = entryParam.getValue("url-index", null);
        md5Index = entryParam.getValue("md5-index", null);
        newKeyIndex = entryParam.getValue("newKey-index", null);
        fopsIndex = entryParam.getValue("fops-index", null);
        pidIndex = entryParam.getValue("persistentId-index", null);
        avnfoIndex = entryParam.getValue("avinfo-index", null);
    }

    private String checkedParseType() throws IOException {
        if (getSourceType().equals("list")) return "object";
        else {
            if ("json".equals(parseType) || "table".equals(parseType) ) {
                return parseType;
            } else {
                throw new IOException("no incorrect parse type, please set it as \"json\" or \"table\".");
            }
        }
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

    private Map<String, String> checkedIndexMap() throws IOException {
        Map<String, String> indexMap = new HashMap<>();
        if (needMd5Index.contains(getProcess())) {
            String md5Index = getMd5Index();
            if (!"".equals(md5Index)) indexMap.put(md5Index, md5Index);
        }
        if (needUrlIndex.contains(getProcess())) {
            String urlIndex = getUrlIndex();
            if (!"".equals(urlIndex)) {
                indexMap.put(urlIndex, urlIndex);
                return indexMap;
            }
        }
        if (needPidIndex.contains(getProcess())) {
            String persistentIdIndex = getPersistentIdIndex();
            if (!"".equals(persistentIdIndex)) {
                indexMap.put(persistentIdIndex, persistentIdIndex);
                return indexMap;
            }
        }
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
        if (needNewKeyIndex.contains(getProcess())) {
            String newKeyIndex = getNewKeyIndex();
            if (!"".equals(newKeyIndex)) {
                indexMap.put(newKeyIndex, newKeyIndex);
                if (indexMap.size() < 2) throw new IOException("please check the key and newKey index, two index can" +
                        "not be same with each other.");
            }
        }
        if (needFopsIndex.contains(getProcess())) {
            String fopsIndex = getFopsIndex();
            if (!"".equals(fopsIndex)) {
                indexMap.put(fopsIndex, fopsIndex);
                if (indexMap.size() < 2) throw new IOException("please check the key and fops index, two index can" +
                        "not be same with each other.");
            }
        }
        return indexMap;
    }

    public String getParseType() throws IOException {
        return checkedParseType();
    }

    public String getSeparator() {
        return separator;
    }

    public Map<String, String> getIndexMap() throws IOException {
        return checkedIndexMap();
    }

    public String getUrlIndex() throws IOException {
        return getIndex(urlIndex, "url");
    }

    public String getMd5Index() throws IOException {
        return getIndex(md5Index, "md5");
    }

    public String getNewKeyIndex() throws IOException {
        return getIndex(newKeyIndex, "newKey");
    }

    public String getFopsIndex() throws IOException {
        return getIndex(fopsIndex, "fops");
    }

    public String getPersistentIdIndex() throws IOException {
        return getIndex(pidIndex, "persistentId");
    }

    public String getAvnfoIndex() throws IOException {
        return getIndex(avnfoIndex, "avinfo");
    }
}
