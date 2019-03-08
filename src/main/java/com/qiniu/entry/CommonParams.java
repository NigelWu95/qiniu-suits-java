package com.qiniu.entry;

import com.google.gson.JsonObject;
import com.qiniu.config.CommandArgs;
import com.qiniu.config.FileProperties;
import com.qiniu.config.JsonFile;
import com.qiniu.service.interfaces.IEntryParam;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.ListBucketUtils;

import java.io.IOException;
import java.util.*;

public class CommonParams {

    private IEntryParam entryParam;
    private String path;
    private String source;
    private Map<String, String> indexMap;
    private String parse;
    private String separator;
    private String accessKey;
    private String secretKey;
    private String bucket;
    private Map<String, String[]> prefixMap;
    private List<String> antiPrefixes;
    private boolean prefixLeft;
    private boolean prefixRight;
    private int unitLen;
    private int threads;
    private int retryCount;
    private boolean saveTotal;
    private String savePath;
    private String saveFormat;
    private String saveSeparator;
    private List<String> rmFields;
    private String process;
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
    private List<String> needBucketProcesses = new ArrayList<String>(){{
        add("status");
        add("type");
        add("lifecycle");
        add("copy");
        add("move");
        add("rename");
        add("delete");
        add("asyncfetch");
        add("pfop");
        add("stat");
    }};
    private List<String> needAuthProcesses = new ArrayList<String>(){{
        addAll(needBucketProcesses);
        add("privateurl");
    }};

    public CommonParams(IEntryParam entryParam) throws IOException {
        this.entryParam = entryParam;
        path = entryParam.getValue("path", null);
        setSource();
        indexMap = new HashMap<>();
        if ("list".equals(source)) {
            accessKey = entryParam.getValue("ak");
            secretKey = entryParam.getValue("sk");
            if (path.startsWith("qiniu://")) bucket = path.substring(8);
            else bucket = entryParam.getValue("bucket");
            antiPrefixes = splitItems(entryParam.getValue("anti-prefixes", ""));
            String prefixes = entryParam.getValue("prefixes", "");
            setPrefixConfig(entryParam.getValue("prefix-config", ""), prefixes);
            setPrefixLeft(entryParam.getValue("prefix-left", ""));
            setPrefixRight(entryParam.getValue("prefix-right", ""));
        } else if ("file".equals(source)) {
            setParse(entryParam.getValue("parse", "table"));
            setSeparator(entryParam.getValue("separator", null));
            setIndexMap();
        }

        setUnitLen(entryParam.getValue("unit-len", "10000"));
        setThreads(entryParam.getValue("threads", "30"));
        setRetryCount(entryParam.getValue("retry-times", "3"));
        // list 操作时默认保存全部原始文件
        setSaveTotal(entryParam.getValue("save-total", String.valueOf("list".equals(getSource()))));
        savePath = entryParam.getValue("save-path", "result");
        saveFormat = entryParam.getValue("save-format", "table");
        // 校验设置的 format 参数
        saveFormat = checked(saveFormat, "save-format", "(csv|table|json)");
        saveSeparator = entryParam.getValue("save-separator", "\t");
        rmFields = Arrays.asList(entryParam.getValue("rm-fields", "").split(","));
        process = entryParam.getValue("process", null);

        if ("file".equals(source) && needBucketProcesses.contains(process)) {
            if (path.startsWith("qiniu://")) bucket = path.substring(8);
            else bucket = entryParam.getValue("bucket");
            if (needAuthProcesses.contains(process)) {
                accessKey = entryParam.getValue("ak");
                secretKey = entryParam.getValue("sk");
            }
        }
    }

    private void setSource() throws IOException {
        try {
            source = entryParam.getValue("source-type");
        } catch (IOException e1) {
            try {
                source = entryParam.getValue("source");
            } catch (IOException e2) {
                if (path == null || path.startsWith("qiniu://")) source = "list";
                else source = "file";
            }
        }
        if (source.matches("(list|file)")) {
            throw new IOException("please set the \"source\" conform to regex:" +
                    " (list|file)");
        }
    }

    private void setParse(String parse) throws IOException {
        this.parse = checked(parse, "parse", "(csv|table|json)");
    }

    private void setSeparator(String separator) {
        if (separator == null) {
            if ("table".equals(parse)) this.separator = "\t";
            else if ("csv".equals(parse)) this.separator = ",";
        } else {
            this.separator = separator;
        }
    }

    private void setUnitLen(String unitLen) throws IOException {
        this.unitLen = Integer.valueOf(checked(unitLen, "unit-len", "\\d+"));
    }

    private void setThreads(String threads) throws IOException {
        this.threads = Integer.valueOf(checked(threads, "threads", "[1-9]\\d*"));
    }

    private void setRetryCount(String retryCount) throws IOException {
        this.retryCount = Integer.valueOf(checked(retryCount, "retry-times", "\\d+"));
    }

    private void setSaveTotal(String saveTotal) throws IOException {
        this.saveTotal = Boolean.valueOf(checked(saveTotal, "save-total", "(true|false)"));
    }

    private void setIndex(String index, String indexName, List<String> needList) throws IOException {
        if (index != null && needList.contains(getProcess())) {
            if (indexMap.containsValue(index)) {
                throw new IOException("the value: " + index + "is already in map: " + indexMap);
            }
            if ("json".equals(parse)) {
                indexMap.put(indexName, index);
            } else if ("table".equals(parse) || "csv".equals(parse)) {
                if (index.matches("\\d")) {
                    indexMap.put(indexName, index);
                } else {
                    throw new IOException("no incorrect " + indexName + "-index, it should be a number.");
                }
            } else {
                // 其他情况暂且忽略该索引
            }
        }
    }

    private void setIndexMap() throws IOException {
        String indexes = entryParam.getValue("indexes", "");
        List<String> keys = Arrays.asList("key", "hash", "fsize", "putTime", "mimeType", "type", "status", "endUser");
        if ("table".equals(parse) || "csv".equals(parse)) {
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

        setIndex(entryParam.getValue("url-index", null), "url", needUrlIndex);
        setIndex(entryParam.getValue("md5-index", null), "md5", needMd5Index);
        setIndex(entryParam.getValue("newKey-index", null), "newKey", needNewKeyIndex);
        setIndex(entryParam.getValue("fops-index", null), "fops", needFopsIndex);
        setIndex(entryParam.getValue("persistentId-index", null), "persistentId", needPidIndex);
        setIndex(entryParam.getValue("avinfo-index", null), "avinfo", needAvinfoIndex);
    }

    private String getMarker(String start, String marker, BucketManager bucketManager) throws IOException {
        if (!"".equals(marker) || "".equals(start)) return marker;
        else {
            FileInfo markerFileInfo = bucketManager.stat(bucket, start);
            markerFileInfo.key = start;
            return ListBucketUtils.calcMarker(markerFileInfo);
        }
    }

    private void setPrefixConfig(String prefixConfig, String prefixes) throws IOException {
        prefixMap = new HashMap<>();
        if (!"".equals(prefixConfig)) {
            JsonFile jsonFile = new JsonFile(prefixConfig);
            JsonObject jsonCfg;
            String marker;
            String end;
            BucketManager manager = new BucketManager(Auth.create(accessKey, secretKey), new Configuration());
            for (String prefix : jsonFile.getJsonObject().keySet()) {
                jsonCfg = jsonFile.getElement(prefix).getAsJsonObject();
                marker = getMarker(jsonCfg.get("start").getAsString(), jsonCfg.get("marker").getAsString(), manager);
                end = jsonCfg.get("end").getAsString();
                prefixMap.put(prefix, new String[]{marker, end});
            }
        } else {
            List<String> prefixList = splitItems(prefixes);
            for (String prefix : prefixList) {
                prefixMap.put(prefix, new String[]{"", ""});
            }
        }
    }

    private List<String> splitItems(String paramLine) {
        if (!"".equals(paramLine)) {
            Set<String> set;
            // 指定前缀包含 "," 号时需要用转义符解决
            if (paramLine.contains("\\,")) {
                String[] elements = paramLine.split("\\\\,");
                set = new HashSet<>(Arrays.asList(elements[0].split(",")));
                set.add(",");
                if (elements.length > 1)set.addAll(Arrays.asList(elements[1].split(",")));
            } else {
                set = new HashSet<>(Arrays.asList(paramLine.split(",")));
            }
            // 删除空前缀的情况避免列举操作时造成误解
            set.remove("");
            return new ArrayList<>(set);
        }
        return new ArrayList<>();
    }

    private void setPrefixLeft(String prefixLeft) throws IOException {
        this.prefixLeft = Boolean.valueOf(checked(prefixLeft, "prefix-left", "(true|false)"));
    }

    private void setPrefixRight(String prefixRight) throws IOException {
        this.prefixRight = Boolean.valueOf(checked(prefixRight, "prefix-right", "(true|false)"));
    }

    public String checked(String param, String name, String conditionReg) throws IOException {
        if (param == null || !param.matches(conditionReg))
            throw new IOException("no correct \"" + name + "\", please set the it conform to regex: " + conditionReg);
        else return param;
    }

    public String getPath() {
        return path;
    }

    public String getSource() {
        return source;
    }

    public String getParse() {
        return parse;
    }

    public String getSeparator() {
        return separator;
    }

    public Map<String, String> getIndexMap() {
        return indexMap;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public List<String> getAntiPrefixes() {
        return antiPrefixes;
    }

    public boolean getPrefixLeft() {
        return prefixLeft;
    }

    public boolean getPrefixRight() {
        return prefixRight;
    }

    public Map<String, String[]> getPrefixMap() {
        return prefixMap;
    }

    public int getUnitLen() {
        return unitLen;
    }

    public int getThreads() {
        return threads;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public Boolean getSaveTotal() {
        return saveTotal;
    }

    public String getSavePath() {
        return savePath;
    }

    public String getSaveFormat() {
        return saveFormat;
    }

    public String getSaveSeparator() {
        return saveSeparator;
    }

    public List<String> getRmFields() {
        return rmFields;
    }

    public String getProcess() {
        return process;
    }
}
