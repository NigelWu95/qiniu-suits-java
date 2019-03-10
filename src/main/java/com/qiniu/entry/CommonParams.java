package com.qiniu.entry;

import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
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
    private String process;
    private String source;
    private String parse;
    private String separator;
    private Map<String, String> indexMap;
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
    private List<String> needUrl = new ArrayList<String>(){{
        add("asyncfetch");
        add("privateurl");
        add("qhash");
        add("avinfo");
    }};
    private List<String> needMd5 = new ArrayList<String>(){{
        add("asyncfetch");
    }};
    private List<String> needNewKey = new ArrayList<String>(){{
        add("rename");
        add("copy");
    }};
    private List<String> needFops = new ArrayList<String>(){{
        add("pfop");
    }};
    private List<String> needPid = new ArrayList<String>(){{
        add("pfopresult");
    }};
    private List<String> needAvinfo = new ArrayList<String>(){{
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
        add("pfop");
        add("stat");
    }};
    private List<String> needAuthProcesses = new ArrayList<String>(){{
        addAll(needBucketProcesses);
        add("asyncfetch");
        add("privateurl");
    }};

    /**
     * 从入口中解析出程序运行所需要的参数，参数解析需要一定的顺序，因为部分参数会依赖前面参数解析的结果
     * @param entryParam 配置参数入口
     * @throws IOException 获取一些参数失败时抛出的异常
     */
    public CommonParams(IEntryParam entryParam) throws IOException {
        this.entryParam = entryParam;
        path = entryParam.getValue("path", "");
        process = entryParam.getValue("process", null);
        setSource();
        if ("list".equals(source)) {
            accessKey = entryParam.getValue("ak");
            secretKey = entryParam.getValue("sk");
            setBucket();
            antiPrefixes = splitItems(entryParam.getValue("anti-prefixes", ""));
            String prefixes = entryParam.getValue("prefixes", "");
            setPrefixConfig(entryParam.getValue("prefix-config", ""), prefixes);
            setPrefixLeft(entryParam.getValue("prefix-left", "false"));
            setPrefixRight(entryParam.getValue("prefix-right", "false"));
        } else if ("file".equals(source)) {
            setParse(entryParam.getValue("parse", "tab"));
            setSeparator(entryParam.getValue("separator", null));
        }
        setIndexMap();
        setUnitLen(entryParam.getValue("unit-len", "10000"));
        setThreads(entryParam.getValue("threads", "30"));
        setRetryCount(entryParam.getValue("retry-times", "3"));
        // list 操作时默认保存全部原始文件
        setSaveTotal(entryParam.getValue("save-total", String.valueOf("list".equals(source))));
        savePath = entryParam.getValue("save-path", "result");
        saveFormat = entryParam.getValue("save-format", "tab");
        // 校验设置的 format 参数
        saveFormat = checked(saveFormat, "save-format", "(csv|tab|json)");
        saveSeparator = entryParam.getValue("save-separator", null);
        setSaveSeparator(saveSeparator);
        rmFields = Arrays.asList(entryParam.getValue("rm-fields", "").split(","));

        if ("file".equals(source) && needBucketProcesses.contains(process)) {
            setBucket();
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
                if ("".equals(path) || path.startsWith("qiniu://")) source = "list";
                else source = "file";
            }
        }
        if (!source.matches("(list|file)")) {
            throw new IOException("please set the \"source\" conform to regex: (list|file)");
        }
    }

    private void setBucket() throws IOException {
        if (path.startsWith("qiniu://")) {
            bucket = path.substring(8);
            bucket = entryParam.getValue("bucket", bucket);
        } else {
            bucket = entryParam.getValue("bucket");
        }
    }

    private void setParse(String parse) throws IOException {
        this.parse = checked(parse, "parse", "(csv|tab|json)");
    }

    private void setSeparator(String separator) {
        if (separator == null) {
            if ("tab".equals(parse)) this.separator = "\t";
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

    private void setSaveSeparator(String separator) {
        if (separator == null) {
            if ("tab".equals(saveFormat)) this.saveSeparator = "\t";
            else if ("csv".equals(saveFormat)) this.saveSeparator = ",";
        } else {
            this.saveSeparator = separator;
        }
    }

    private void setIndex(String indexName, String index, boolean check) throws IOException {
        if (indexName != null && !"-1".equals(indexName) && check) {
            if (indexMap.containsKey(indexName)) {
                throw new IOException("the value: " + indexName + "is already in map: " + indexMap);
            }
            if ("json".equals(parse)) {
                indexMap.put(indexName, index);
            } else if ("tab".equals(parse) || "csv".equals(parse)) {
                if (indexName.matches("\\d+")) {
                    indexMap.put(indexName, index);
                } else {
                    throw new IOException("incorrect " + index + "-index: " + indexName + ", it should be a number.");
                }
            } else {
                throw new IOException("the parse type: " + parse + " is unsupported now.");
            }
        }
    }

    private void setIndexMap() throws IOException {
        indexMap = new HashMap<>();
        List<String> keys = Arrays.asList("key", "hash", "fsize", "putTime", "mimeType", "type", "status", "endUser");
        if ("list".equals(source)) {
            for (String key : keys) {
                indexMap.put(key, key);
            }
        } else {
            setIndex(entryParam.getValue("url-index", null), "url", needUrl.contains(process));
            setIndex(entryParam.getValue("md5-index", null), "md5", needMd5.contains(process));
            setIndex(entryParam.getValue("newKey-index", null), "newKey", needNewKey.contains(process));
            setIndex(entryParam.getValue("fops-index", null), "fops", needFops.contains(process));
            setIndex(entryParam.getValue("persistentId-index", null), "pid", needPid.contains(process));
            setIndex(entryParam.getValue("avinfo-index", null), "avinfo", needAvinfo.contains(process));

            List<String> indexList = splitItems(entryParam.getValue("indexes", ""));
            if (indexList.size() > 8) {
                throw new IOException("the file info's index length is too long.");
            } else {
                for (int i = 0; i < indexList.size(); i++) {
                    setIndex(indexList.get(i), keys.get(i), true);
                }
            }
        }
        // 默认索引
        if (indexMap.size() == 0) {
            indexMap.put("json".equals(parse) ? "key" : "0", "key");
        }
    }

    private String getMarker(String start, String marker, BucketManager bucketManager) throws IOException {
        if (!"".equals(marker) || "".equals(start)) return marker;
        else {
            try {
                FileInfo markerFileInfo = bucketManager.stat(bucket, start);
                markerFileInfo.key = start;
                return ListBucketUtils.calcMarker(markerFileInfo);
            } catch (QiniuException e) {
                if (e.code() == 612) {
                    throw new IOException("start: \"" + start + "\", can not get invalid marker because " + e.error());
                } else {
                    throw e;
                }
            }
        }
    }

    private void setPrefixConfig(String prefixConfig, String prefixes) throws IOException {
        prefixMap = new HashMap<>();
        if (!"".equals(prefixConfig) && prefixConfig != null) {
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
                // 如果前面前面位置已存在该 prefix，则通过 remove 操作去重，使用后面的覆盖前面的
                prefixMap.remove(prefix);
                prefixMap.put(prefix, new String[]{"", ""});
            }
        }
    }

    public List<String> splitItems(String paramLine) {
        List<String> itemList = new ArrayList<>();
        String[] items = new String[]{};
        if (!"".equals(paramLine) && paramLine != null) {
            // 指定前缀包含 "," 号时需要用转义符解决
            if (paramLine.contains("\\,")) {
                String[] elements = paramLine.split("\\\\,");
                String[] items1 = elements[0].split(",");
                if (elements.length > 1) {
                    String[] items2 = elements[1].split(",");
                    items = new String[items1.length + items2.length + 1];
                    System.arraycopy(items1, 0, items, 0, items1.length);
                    items[items1.length] = "";
                    System.arraycopy(items2, 0, items, items1.length + 1, items2.length + 1);
                } else {
                    items = new String[items1.length];
                    System.arraycopy(items1, 0, items, 0, items1.length);
                }
            } else {
                items = paramLine.split(",");
            }
        }
        // itemList 不能去重，因为要用于解析 indexes 设置，可能存在同时使用多个 "-1" 来跳过某些字段
        for (String item : items) {
            if (!"".equals(item)) itemList.add(item);
        }
        return itemList;
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
