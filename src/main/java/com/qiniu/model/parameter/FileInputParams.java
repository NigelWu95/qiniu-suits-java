package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;
import java.util.*;

public class FileInputParams extends CommonParams {

    private String filePath;
    private String separator;
    private String indexes;
    private String urlIndex;
    private String md5Index;
    private String newKeyIndex;
    private String fopsIndex;
    private String persistentIdIndex;
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
    }};
    private List<String> needFopsIndex = new ArrayList<String>(){{
        add("pfop");
    }};
    private List<String> needPersistentIdIndex = new ArrayList<String>(){{
        add("pfopresult");
    }};

    public FileInputParams(IEntryParam entryParam) throws IOException {
        super(entryParam);
        try { this.filePath = entryParam.getParamValue("file-path");} catch (Exception e) {}
        try { this.separator = entryParam.getParamValue("in-separator"); } catch (Exception e) {}
        try { this.indexes = entryParam.getParamValue("indexes"); } catch (Exception e) { indexes = ""; }
        try { this.urlIndex = entryParam.getParamValue("url-index"); } catch (Exception e) { urlIndex = ""; }
        try { this.md5Index = entryParam.getParamValue("md5-index"); } catch (Exception e) { md5Index = ""; }
        try { this.newKeyIndex = entryParam.getParamValue("newKey-index"); } catch (Exception e) { newKeyIndex = ""; }
        try { this.fopsIndex = entryParam.getParamValue("fops-index"); } catch (Exception e) { fopsIndex = ""; }
        try { this.persistentIdIndex = entryParam.getParamValue("persistentId-index"); } catch (Exception e) { persistentIdIndex = ""; }
    }

    public String getFilePath() throws IOException {
        if (filePath == null || "".equals(filePath)) throw new IOException("please set the file path.");
        else if (filePath.startsWith("/")) throw new IOException("the file path only support relative path.");
        return filePath;
    }

    public String getSeparator() {
        if (separator == null || "".equals(separator)) {
            return "\t";
        } else {
            return separator;
        }
    }

    public Boolean getSaveTotal() {
        if (saveTotal.matches("(true|false)")) {
            return Boolean.valueOf(saveTotal);
        } else {
            return false;
        }
    }

    public Map<String, String> getIndexMap() throws IOException {
        Map<String, String> indexMap = new HashMap<>();
        List<String> keys = Arrays.asList("key", "hash", "fsize", "putTime", "mimeType", "endUser", "type", "status");
        if ("table".equals(getParseType())) {
            if ("".equals(indexes) || indexes.matches("(\\d+,)*\\d")) {
                List<String> indexList = Arrays.asList(indexes.split(","));
                if (indexList.size() == 0) {
                    indexMap.put("0", keys.get(0));
                } else if (indexList.size() > 8) {
                    throw new IOException("the file info's index length is too long.");
                } else {
                    for (int i = 0; i < indexList.size(); i++) { indexMap.put(indexList.get(i), keys.get(i)); }
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
        if (needUrlIndex.contains(getProcess())) {
            String urlIndex = getUrlIndex();
            if (!"".equals(urlIndex)) indexMap.put(urlIndex, urlIndex);
        }
        if (needMd5Index.contains(getProcess())) {
            String md5Index = getMd5Index();
            if (!"".equals(md5Index)) indexMap.put(md5Index, md5Index);
        }
        if (needNewKeyIndex.contains(getProcess())) {
            String newKeyIndex = getNewKeyIndex();
            if (!"".equals(newKeyIndex)) indexMap.put(newKeyIndex, newKeyIndex);
        }
        if (needFopsIndex.contains(getProcess())) {
            String fopsIndex = getFopsIndex();
            if (!"".equals(fopsIndex)) indexMap.put(fopsIndex, fopsIndex);
        }
        if (needPersistentIdIndex.contains(getProcess())) {
            String persistentIdIndex = getPersistentIdIndex();
            if (!"".equals(persistentIdIndex)) indexMap.put(persistentIdIndex, persistentIdIndex);
        }
        return indexMap;
    }

    public String getUrlIndex() throws IOException {
        if ("json".equals(getParseType())) {
            return urlIndex;
        } else if ("table".equals(getParseType())) {
            if ("".equals(urlIndex) || urlIndex.matches("\\d")) {
                return urlIndex;
            } else {
                throw new IOException("no incorrect url index, it should be a number.");
            }
        } else {
            // 其他情况忽略该索引
            return "";
        }
    }

    public String getMd5Index() throws IOException {
        if ("json".equals(getParseType())) {
            return md5Index;
        } else if ("table".equals(getParseType())) {
            if ("".equals(md5Index) || md5Index.matches("\\d")) {
                return md5Index;
            } else {
                throw new IOException("no incorrect md5 index, it should be a number.");
            }
        } else {
            // 其他情况忽略该索引
            return "";
        }
    }

    public String getNewKeyIndex() throws IOException {
        if ("json".equals(getParseType())) {
            return newKeyIndex;
        } else if ("table".equals(getParseType())) {
            if ("".equals(newKeyIndex) || newKeyIndex.matches("\\d")) {
                return newKeyIndex;
            } else {
                throw new IOException("no incorrect newKey index, it should be a number.");
            }
        } else {
            // 其他情况忽略该索引
            return "";
        }
    }

    public String getFopsIndex() throws IOException {
        if ("json".equals(getParseType())) {
            return fopsIndex;
        } else if ("table".equals(getParseType())) {
            if ("".equals(fopsIndex) || fopsIndex.matches("\\d")) {
                return fopsIndex;
            } else {
                throw new IOException("no incorrect fops index, it should be a number.");
            }
        } else {
            // 其他情况忽略该索引
            return "";
        }
    }

    public String getPersistentIdIndex() throws IOException {
        if ("json".equals(getParseType())) {
            return persistentIdIndex;
        } else if ("table".equals(getParseType())) {
            if ("".equals(persistentIdIndex) || persistentIdIndex.matches("\\d")) {
                return persistentIdIndex;
            } else {
                throw new IOException("no incorrect persistentId index, it should be a number.");
            }
        } else {
            // 其他情况忽略该索引
            return "";
        }
    }
}
