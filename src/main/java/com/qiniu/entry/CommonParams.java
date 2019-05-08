package com.qiniu.entry;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.common.Constants;
import com.qiniu.config.JsonFile;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.process.filtration.BaseFilter;
import com.qiniu.process.filtration.SeniorFilter;
import com.qiniu.util.*;
import com.qiniu.util.Base64;

import java.io.IOException;
import java.util.*;

public class CommonParams {

    private IEntryParam entryParam;
    private int connectTimeout;
    private int readTimeout;
    private int requestTimeout;
    private String path;
    private BaseFilter<Map<String, String>> baseFilter;
    private SeniorFilter<Map<String, String>> seniorFilter;
    private String process;
    private String addKeyPrefix;
    private String rmKeyPrefix;
    private String source;
    private String parse;
    private String separator;
    private String qiniuAccessKey;
    private String qiniuSecretKey;
    private String tencentSecretId;
    private String tencentSecretKey;
    private String aliyunAccessId;
    private String aliyunAccessSecret;
    private String bucket;
    private String regionName;
    private Map<String, String[]> prefixesMap;
    private List<String> antiPrefixes;
    private boolean prefixLeft;
    private boolean prefixRight;
    private int unitLen;
    private int threads;
    private int batchSize;
    private int retryTimes;
    private boolean saveTotal;
    private String savePath;
    private String saveTag;
    private String saveFormat;
    private String saveSeparator;
    private List<String> rmFields;
    private HashMap<String, String> indexMap;

    /**
     * 从入口中解析出程序运行所需要的参数，参数解析需要一定的顺序，因为部分参数会依赖前面参数解析的结果
     * @param entryParam 配置参数入口
     * @throws IOException 获取一些参数失败时抛出的异常
     */
    public CommonParams(IEntryParam entryParam) throws Exception {
        this.entryParam = entryParam;
        connectTimeout = Integer.valueOf(entryParam.getValue("connect-timeout", "60").trim());
        readTimeout = Integer.valueOf(entryParam.getValue("read-timeout", "120").trim());
        requestTimeout = Integer.valueOf(entryParam.getValue("request-timeout", "60").trim());
        path = entryParam.getValue("path", "");
        process = entryParam.getValue("process", "").trim();
        addKeyPrefix = entryParam.getValue("add-keyPrefix", null);
        rmKeyPrefix = entryParam.getValue("rm-keyPrefix", null);
        setSource();
        setBaseFilter();
        setSeniorFilter();
        if ("local".equals(source)) {
            setParse(entryParam.getValue("parse", "tab").trim());
            setSeparator(entryParam.getValue("separator", null));
            if (ProcessUtils.needBucket(process)) bucket = entryParam.getValue("bucket").trim();
            if (ProcessUtils.needAuth(process)) {
                qiniuAccessKey = entryParam.getValue("ak").trim();
                qiniuSecretKey = entryParam.getValue("sk").trim();
            }
        } else {
            if ("tencent".equals(source)) {
                tencentSecretId = entryParam.getValue("ten-id").trim();
                tencentSecretKey = entryParam.getValue("ten-secret").trim();
                regionName = entryParam.getValue("region").trim();
            } else if ("aliyun".equals(source)) {
                aliyunAccessId = entryParam.getValue("ali-id").trim();
                aliyunAccessSecret = entryParam.getValue("ali-secret").trim();
                regionName = entryParam.getValue("region").trim();
                if (!regionName.startsWith("oss-")) regionName = "oss-" + regionName;
            } else {
                qiniuAccessKey = entryParam.getValue("ak").trim();
                qiniuSecretKey = entryParam.getValue("sk").trim();
                regionName = entryParam.getValue("region", "auto").trim();
            }
            setBucket();
            parse = "object";
            antiPrefixes = splitItems(entryParam.getValue("anti-prefixes", ""));
            String prefixes = entryParam.getValue("prefixes", "");
            setPrefixesMap(entryParam.getValue("prefix-config", ""), prefixes);
            setPrefixLeft(entryParam.getValue("prefix-left", "false").trim());
            setPrefixRight(entryParam.getValue("prefix-right", "false").trim());
        }

        setUnitLen(entryParam.getValue("unit-len", "-1").trim());
        setThreads(entryParam.getValue("threads", "30").trim());
        setRetryTimes(entryParam.getValue("retry-times", "3").trim());
        setBatchSize(entryParam.getValue("batch-size", "-1").trim());
        // list 操作时默认保存全部原始文件
        setSaveTotal(entryParam.getValue("save-total", "").trim());
        savePath = entryParam.getValue("save-path", "local".equals(source) ? (path.endsWith("/") ?
                path.substring(0, path.length() - 1) : path) + "-result" : bucket);
        saveTag = entryParam.getValue("save-tag", "").trim();
        saveFormat = entryParam.getValue("save-format", "tab").trim();
        // 校验设置的 format 参数
        saveFormat = checked(saveFormat, "save-format", "(csv|tab|json)");
        saveSeparator = entryParam.getValue("save-separator", null);
        setSaveSeparator(saveSeparator);
        rmFields = Arrays.asList(entryParam.getValue("rm-fields", "").trim().split(","));
        setIndexMap();
    }

    private void setSource() throws IOException {
        try {
            source = entryParam.getValue("source-type").trim();
        } catch (IOException e1) {
            try {
                source = entryParam.getValue("source").trim();
            } catch (IOException e2) {
                if ("".equals(path) || path.startsWith("qiniu://")) source = "qiniu";
                else if (path.startsWith("tencent://")) source = "tencent";
                else if (path.startsWith("aliyun://")) source = "aliyun";
                else source = "local";
            }
        }
        // list 和 file 方式是兼容老的数据源参数，list 默认表示从七牛进行列举，file 表示从本地读取文件
        if ("list".equals(source)) source = "qiniu";
        else if ("file".equals(source)) source = "local";
        if (!source.matches("(local|qiniu|tencent|aliyun)")) {
            throw new IOException("please set the \"source\" conform to regex: (local|qiniu|tencent|aliyun)");
        }
    }

    private void setBaseFilter() throws Exception {
        String keyPrefix = entryParam.getValue("f-prefix", "");
        String keySuffix = entryParam.getValue("f-suffix", "");
        String keyInner = entryParam.getValue("f-inner", "");
        String keyRegex = entryParam.getValue("f-regex", "");
        String mimeType = entryParam.getValue("f-mime", "");
        String antiKeyPrefix = entryParam.getValue("f-anti-prefix", "");
        String antiKeySuffix = entryParam.getValue("f-anti-suffix", "");
        String antiKeyInner = entryParam.getValue("f-anti-inner", "");
        String antiKeyRegex = entryParam.getValue("f-anti-regex", "");
        String antiMimeType = entryParam.getValue("f-anti-mime", "");
        String[] dateScale = splitDateScale(entryParam.getValue("f-date-scale", "").trim());
        long putTimeMin = checkedDatetime(dateScale[0]);
        long putTimeMax = checkedDatetime(dateScale[1]);
        if (putTimeMax != 0 && putTimeMax <= putTimeMin ) {
            throw new IOException("please set date scale to make first as start date, next as end date, <date1> " +
                    "should earlier then <date2>.");
        }
        String type = entryParam.getValue("f-type", "").trim();
        String status = entryParam.getValue("f-status", "").trim();
        if (!"".equals(type)) type = checked(type, "f-type", "[01]");
        if (!"".equals(status)) status = checked(status, "f-status", "[01]");

        List<String> keyPrefixList = getFilterList("key", keyPrefix, "prefix");
        List<String> keySuffixList = getFilterList("key", keySuffix, "suffix");
        List<String> keyInnerList = getFilterList("key", keyInner, "inner");
        List<String> keyRegexList = getFilterList("key", keyRegex, "regex");
        List<String> mimeTypeList = getFilterList("mimeType", mimeType, "mime");
        List<String> antiKeyPrefixList = getFilterList("key", antiKeyPrefix, "anti-prefix");
        List<String> antiKeySuffixList = getFilterList("key", antiKeySuffix, "anti-suffix");
        List<String> antiKeyInnerList = getFilterList("key", antiKeyInner, "anti-inner");
        List<String> antiKeyRegexList = getFilterList("key", antiKeyRegex, "anti-regex");
        List<String> antiMimeTypeList = getFilterList("mimeType", antiMimeType, "anti-mime");
        try {
            baseFilter = new BaseFilter<Map<String, String>>(keyPrefixList, keySuffixList, keyInnerList, keyRegexList,
                    antiKeyPrefixList, antiKeySuffixList, antiKeyInnerList, antiKeyRegexList, mimeTypeList, antiMimeTypeList,
                    putTimeMin, putTimeMax, type, status) {
                @Override
                protected boolean checkItem(Map<String, String> item, String key) {
                    return item == null || item.get(key) == null;
                }
                @Override
                protected String valueFrom(Map<String, String> item, String key) {
                    return item.get(key);
                }
            };
        } catch (IOException e) {
            baseFilter = null;
        }
    }

    private void setSeniorFilter() throws IOException {
        String checkType = entryParam.getValue("f-check", "").trim();
        checkType = checked(checkType, "f-check", "(|ext-mime)").trim();
        String checkConfig = entryParam.getValue("f-check-config", "");
        String checkRewrite = entryParam.getValue("f-check-rewrite", "false").trim();
        checkRewrite = checked(checkRewrite, "f-check-rewrite", "(true|false)");
        try {
            seniorFilter = new SeniorFilter<Map<String, String>>(checkType, checkConfig, Boolean.valueOf(checkRewrite)) {
                @Override
                protected String valueFrom(Map<String, String> item, String key) {
                    return item != null ? item.get(key) : null;
                }
            };
        } catch (IOException e) {
            seniorFilter = null;
        }
    }

    /**
     * 支持从路径方式上解析出 bucket，如果主动设置 bucket 则替换路径中的值
     * @throws IOException 解析 bucket 参数失败抛出异常
     */
    private void setBucket() throws IOException {
        if ("qiniu".equals(source) && path.startsWith("qiniu://")) {
            bucket = path.substring(8);
            bucket = entryParam.getValue("bucket", bucket).trim();
        } else if ("tencent".equals(source) && path.startsWith("tencent://")) {
            bucket = path.substring(10);
            bucket = entryParam.getValue("bucket", bucket).trim();
        } else if ("aliyun".equals(source) && path.startsWith("aliyun://")) {
            bucket = path.substring(9);
            bucket = entryParam.getValue("bucket", bucket).trim();
        } else {
            bucket = entryParam.getValue("bucket").trim();
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
        if ("-1".equals(unitLen)) {
            if ("qiniu".equals(source) || "local".equals(source)) unitLen = "10000";
            else unitLen = "1000";
        }
        this.unitLen = Integer.valueOf(checked(unitLen, "unit-len", "\\d+"));
    }

    private void setThreads(String threads) throws IOException {
        this.threads = Integer.valueOf(checked(threads, "threads", "[1-9]\\d*"));
    }

    private void setBatchSize(String batchSize) throws IOException {
        if ("-1".equals(batchSize)) {
            if (ProcessUtils.canBatch(process)) {
                batchSize = "stat".equals(process) ? "100" : "1000";
            } else {
                batchSize = "0";
            }
        }
        this.batchSize = Integer.valueOf(checked(batchSize, "batch-size", "\\d+"));
    }

    private void setRetryTimes(String retryTimes) throws IOException {
        this.retryTimes = Integer.valueOf(checked(retryTimes, "retry-times", "\\d+"));
    }

    private void setSaveTotal(String saveTotal) throws IOException {
        if (saveTotal == null || "".equals(saveTotal)) {
            if (source.matches("(qiniu|tencent|aliyun)")) {
                if (process == null || "".equals(process)) {
                    saveTotal = "true";
                } else {
                    if (baseFilter != null || seniorFilter != null) saveTotal = "true";
                    else saveTotal = "false";
                }
            } else {
                if ((process != null && !"".equals(process)) || baseFilter != null || seniorFilter != null) saveTotal = "false";
                else saveTotal = "true";
            }
        }
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

    private void setPrefixesMap(String prefixConfig, String prefixes) throws IOException {
        prefixesMap = new HashMap<>();
        if (!"".equals(prefixConfig) && prefixConfig != null) {
            JsonFile jsonFile = new JsonFile(prefixConfig);
            JsonObject jsonCfg;
            String marker = null;
            String end;
            for (String prefix : jsonFile.getJsonObject().keySet()) {
                jsonCfg = jsonFile.getElement(prefix).getAsJsonObject();
                if (jsonCfg.get("marker") instanceof JsonNull || "".equals(jsonCfg.get("marker").getAsString())) {
                    if (jsonCfg.get("start") instanceof JsonNull || "".equals(jsonCfg.get("start").getAsString())) {
                        marker = "";
                    } else {
                        if ("qiniu".equals(source)) {
                            JsonObject jsonObject = new JsonObject();
                            jsonObject.addProperty("k", jsonCfg.get("start").getAsString());
                            marker = Base64.encodeToString(JsonConvertUtils.toJson(jsonObject).getBytes(Constants.UTF_8),
                                    Base64.URL_SAFE | Base64.NO_WRAP);
                        } else if ("tencent".equals(source)) {
                            marker = jsonCfg.get("start").getAsString();
                        } else if ("aliyun".equals(source)) {
                            marker = jsonCfg.get("start").getAsString();
                        }
                    }
                } else {
                    marker = jsonCfg.get("marker").getAsString();
                }
                end = jsonCfg.get("end").getAsString();
                prefixesMap.put(prefix, new String[]{marker, end});
            }
        } else {
            List<String> prefixList = splitItems(prefixes);
            for (String prefix : prefixList) {
                // 如果前面前面位置已存在该 prefix，则通过 remove 操作去重，使用后面的覆盖前面的
                prefixesMap.remove(prefix);
                prefixesMap.put(prefix, new String[]{"", ""});
            }
        }
    }

    public List<String> splitItems(String paramLine) {
        List<String> itemList = new ArrayList<>();
        String[] items = new String[]{};
        if (paramLine != null && !"".equals(paramLine)) {
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

    private void setIndex(String index, String indexName, boolean check) throws IOException {
        if (check && indexMap.containsKey(index)) {
            throw new IOException("index: " + index + " is already used by \"" + indexMap.get(index) + "-index=" + index + "\"");
        }
        if (index != null && !"".equals(index) && !"-1".equals(index)) {
            if ("json".equals(parse) || "object".equals(parse)) {
                indexMap.put(index, indexName);
            } else if ("tab".equals(parse) || "csv".equals(parse)) {
                if (index.matches("\\d+")) {
                    indexMap.put(index, indexName);
                } else {
                    throw new IOException("incorrect " + indexName + "-index: " + index + ", it should be a number.");
                }
            } else {
                throw new IOException("the parse type: " + parse + " is unsupported now.");
            }
        }
    }

    private void setIndexMap() throws IOException {
        indexMap = new HashMap<>();
        List<String> keys = LineUtils.fileInfoFields;
        List<String> indexList = splitItems(entryParam.getValue("indexes", "").trim());
        if (indexList.size() > 9) {
            throw new IOException("the file info's index length is too long.");
        } else {
            for (int i = 0; i < indexList.size(); i++) {
                setIndex(indexList.get(i), keys.get(i), true);
            }
        }

        if ("local".equals(source)) {
            setIndex(entryParam.getValue("url-index", "").trim(), "url", ProcessUtils.needUrl(process));
            setIndex(entryParam.getValue("newKey-index", "").trim(), "newKey", ProcessUtils.needNewKey(process));
            setIndex(entryParam.getValue("fops-index", "").trim(), "fops", ProcessUtils.needFops(process));
            setIndex(entryParam.getValue("persistentId-index", "").trim(), "pid", ProcessUtils.needPid(process));
            setIndex(entryParam.getValue("avinfo-index", "").trim(), "avinfo", ProcessUtils.needAvinfo(process));
            // 默认索引包含 key
            if (!indexMap.containsValue("key")) {
                try {
                    setIndex("json".equals(parse) ? "key" : "0", "key", true);
                } catch (IOException e) {
                    throw new IOException("you need to set indexes with key's index not default value, " +
                            "because the default key's" + e.getMessage());
                }
            }
        } else {
            // 资源列举情况下设置默认索引
            if (indexMap.size() == 0) {
                if (ProcessUtils.supportListSource(process)) {
                    indexMap.put("key", "key");
                    if (baseFilter != null) {
                        if (baseFilter.checkMimeTypeCon()) indexMap.put("mimeType", "mimeType");
                        if (baseFilter.checkPutTimeCon()) indexMap.put("putTime", "putTime");
                        if (baseFilter.checkTypeCon()) indexMap.put("type", "type");
                        if (baseFilter.checkStatusCon()) indexMap.put("status", "status");
                    }
                    if (seniorFilter != null) {
                        if (seniorFilter.checkExtMime()) indexMap.put("mimeType", "mimeType");
                    }
                } else {
                    for (String key : keys) {
                        indexMap.put(key, key);
                    }
                }
            }
            if (ProcessUtils.supportListSource(process)) {
                if (!indexMap.containsValue("key"))
                    throw new IOException("please check your indexes settings, miss a key index in first position.");
            } else if (process != null && !"".equals(process)) {
                throw new IOException("the process: " + process + " don't support getting source line from list.");
            }
        }
    }

    public boolean containIndex(String name) {
        return indexMap.containsValue(name);
    }

    public String checked(String param, String name, String conditionReg) throws IOException {
        if (param == null || !param.matches(conditionReg))
            throw new IOException("no correct \"" + name + "\", please set the it conform to regex: " + conditionReg);
        else return param;
    }

    public List<String> getFilterList(String key, String field, String name)
            throws IOException {
        if (!"".equals(field)) {
            if (indexMap == null || indexMap.containsValue(key)) {
                return splitItems(field);
            } else {
                throw new IOException("f-" + name + " filter must get the " + key + "'s index in indexes settings." +
                        " the default indexes setting only contains \"key\"");
            }
        } else return null;
    }

    public String[] splitDateScale(String dateScale) throws IOException {
        String[] scale;
        if (dateScale != null && !"".equals(dateScale)) {
            // 设置的 dateScale 格式应该为 [yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss]
            if (dateScale.startsWith("[") && dateScale.endsWith("]")) {
                scale = dateScale.substring(1, dateScale.length() - 1).split(",");
            } else if (dateScale.startsWith("[") || dateScale.endsWith("]")) {
                throw new IOException("please check your date scale, set it as \"[<date1>,<date2>]\".");
            } else {
                scale = dateScale.split(",");
            }
            if (indexMap != null && indexMap.containsValue("putTime")) {
                throw new IOException("f-date-scale filter must get the putTime's index in indexes settings." +
                        " the default indexes setting only contains \"key\".");
            }
        } else {
            scale = new String[]{"", ""};
        }
        if (scale.length <= 1) {
            throw new IOException("please set start and end date, if no start please set is as \"[0,<date>]\"");
        }
        return scale;
    }

    public Long checkedDatetime(String datetime) throws Exception {
        long time;
        if (datetime == null ||datetime.matches("(|0)")) {
            time = 0L;
        } else if (datetime.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
            time = DateUtils.parseYYYYMMDDHHMMSSdatetime(datetime);
        } else if (datetime.matches("\\d{4}-\\d{2}-\\d{2}")) {
            time = DateUtils.parseYYYYMMDDHHMMSSdatetime(datetime + " 00:00:00");
        } else {
            throw new IOException("please check your datetime string format, set it as \"yyyy-MM-dd HH:mm:ss\".");
        }
        if (time > 0L && indexMap != null && !indexMap.containsValue("putTime")) {
            throw new IOException("f-date filter must get the putTime's index.");
        }
        return time * 10000;
    }

    public void setEntryParam(IEntryParam entryParam) {
        this.entryParam = entryParam;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setBaseFilter(BaseFilter<Map<String, String>> baseFilter) {
        this.baseFilter = baseFilter;
    }

    public void setSeniorFilter(SeniorFilter<Map<String, String>> seniorFilter) {
        this.seniorFilter = seniorFilter;
    }

    public void setProcess(String process) {
        this.process = process;
    }

    public void setAddKeyPrefix(String addKeyPrefix) {
        this.addKeyPrefix = addKeyPrefix;
    }

    public void setRmKeyPrefix(String rmKeyPrefix) {
        this.rmKeyPrefix = rmKeyPrefix;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setIndexMap(HashMap<String, String> indexMap) {
        this.indexMap = indexMap;
    }

    public void setQiniuAccessKey(String qiniuAccessKey) {
        this.qiniuAccessKey = qiniuAccessKey;
    }

    public void setQiniuSecretKey(String qiniuSecretKey) {
        this.qiniuSecretKey = qiniuSecretKey;
    }

    public void setTencentSecretId(String tencentSecretId) {
        this.tencentSecretId = tencentSecretId;
    }

    public void setTencentSecretKey(String tencentSecretKey) {
        this.tencentSecretKey = tencentSecretKey;
    }

    public void setAliyunAccessId(String aliyunAccessId) {
        this.aliyunAccessId = aliyunAccessId;
    }

    public void setAliyunAccessSecret(String aliyunAccessSecret) {
        this.aliyunAccessSecret = aliyunAccessSecret;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public void setPrefixesMap(Map<String, String[]> prefixesMap) {
        this.prefixesMap = prefixesMap;
    }

    public void setAntiPrefixes(List<String> antiPrefixes) {
        this.antiPrefixes = antiPrefixes;
    }

    public void setPrefixLeft(boolean prefixLeft) {
        this.prefixLeft = prefixLeft;
    }

    public void setPrefixRight(boolean prefixRight) {
        this.prefixRight = prefixRight;
    }

    public void setUnitLen(int unitLen) {
        this.unitLen = unitLen;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public void setSaveTotal(boolean saveTotal) {
        this.saveTotal = saveTotal;
    }

    public void setSavePath(String savePath) {
        this.savePath = savePath;
    }

    public void setSaveTag(String saveTag) {
        this.saveTag = saveTag;
    }

    public void setSaveFormat(String saveFormat) {
        this.saveFormat = saveFormat;
    }

    public void setRmFields(List<String> rmFields) {
        this.rmFields = rmFields;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public String getPath() {
        return path;
    }

    public String getProcess() {
        return process;
    }

    public String getAddKeyPrefix() {
        return addKeyPrefix;
    }

    public String getRmKeyPrefix() {
        return rmKeyPrefix;
    }

    public String getSource() {
        return source;
    }

    public BaseFilter<Map<String, String>> getBaseFilter() {
        return baseFilter;
    }

    public SeniorFilter<Map<String, String>> getSeniorFilter() {
        return seniorFilter;
    }

    public String getParse() {
        return parse;
    }

    public String getSeparator() {
        return separator;
    }

    public String getQiniuAccessKey() {
        return qiniuAccessKey;
    }

    public String getQiniuSecretKey() {
        return qiniuSecretKey;
    }

    public String getTencentSecretId() {
        return tencentSecretId;
    }

    public String getTencentSecretKey() {
        return tencentSecretKey;
    }

    public String getAliyunAccessId() {
        return aliyunAccessId;
    }

    public String getAliyunAccessSecret() {
        return aliyunAccessSecret;
    }

    public String getBucket() {
        return bucket;
    }

    public String getRegionName() {
        return regionName;
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

    public Map<String, String[]> getPrefixesMap() {
        return prefixesMap;
    }

    public int getUnitLen() {
        return unitLen;
    }

    public int getThreads() {
        return threads;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public Boolean getSaveTotal() {
        return saveTotal;
    }

    public String getSavePath() {
        return savePath;
    }

    public String getSaveTag() {
        return saveTag;
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

    public HashMap<String, String> getIndexMap() {
        return indexMap;
    }
}
