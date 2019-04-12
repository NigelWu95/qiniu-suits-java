package com.qiniu.entry;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.config.JsonFile;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.process.filtration.BaseFieldsFilter;
import com.qiniu.process.filtration.SeniorChecker;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.*;

public class CommonParams {

    private IEntryParam entryParam;
    private String path;
    private BaseFieldsFilter baseFieldsFilter;
    private SeniorChecker seniorChecker;
    private String process;
    private String rmKeyPrefix;
    private String source;
    private String parse;
    private String separator;
    private String qiniuAccessKey;
    private String qiniuSecretKey;
    private String tencentSecretId;
    private String tencentSecretKey;
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
        path = entryParam.getValue("path", "");
        process = entryParam.getValue("process", null);
        rmKeyPrefix = entryParam.getValue("rm-keyPrefix", null);
        setSource();
        setBaseFieldsFilter();
        setSeniorChecker();
        if ("local".equals(source)) {
            setParse(entryParam.getValue("parse", "tab"));
            setSeparator(entryParam.getValue("separator", null));
            if (ProcessUtils.needBucket(process)) setBucket();
            if (ProcessUtils.needAuth(process)) {
                qiniuAccessKey = entryParam.getValue("ak");
                qiniuSecretKey = entryParam.getValue("sk");
            }
        } else {
            if ("qiniu".equals(source)) {
                qiniuAccessKey = entryParam.getValue("ak");
                qiniuSecretKey = entryParam.getValue("sk");
                regionName = entryParam.getValue("region", "auto");
            } else if ("tencent".equals(source)) {
                tencentSecretId = entryParam.getValue("t-sid");
                tencentSecretKey = entryParam.getValue("t-sk");
                regionName = entryParam.getValue("region");
            }
            setBucket();
            parse = "object";
            antiPrefixes = splitItems(entryParam.getValue("anti-prefixes", ""));
            String prefixes = entryParam.getValue("prefixes", "");
            setPrefixesMap(entryParam.getValue("prefix-config", ""), prefixes);
            setPrefixLeft(entryParam.getValue("prefix-left", "false"));
            setPrefixRight(entryParam.getValue("prefix-right", "false"));
        }

        setUnitLen(entryParam.getValue("unit-len", "10000"));
        setThreads(entryParam.getValue("threads", "30"));
        setRetryTimes(entryParam.getValue("retry-times", "3"));
        setBatchSize(entryParam.getValue("batch-size", "-1"));
        // list 操作时默认保存全部原始文件
        setSaveTotal(entryParam.getValue("save-total", null));
        savePath = entryParam.getValue("save-path", "result");
        saveTag = entryParam.getValue("save-tag", "");
        saveFormat = entryParam.getValue("save-format", "tab");
        // 校验设置的 format 参数
        saveFormat = checked(saveFormat, "save-format", "(csv|tab|json)");
        saveSeparator = entryParam.getValue("save-separator", null);
        setSaveSeparator(saveSeparator);
        rmFields = Arrays.asList(entryParam.getValue("rm-fields", "").split(","));
        setIndexMap();
    }

    private void setSource() throws IOException {
        try {
            source = entryParam.getValue("source-type");
        } catch (IOException e1) {
            try {
                source = entryParam.getValue("source");
            } catch (IOException e2) {
                if ("".equals(path) || path.startsWith("qiniu://")) source = "qiniu";
                else if (path.startsWith("tencent://")) source = "tencent";
                else source = "local";
            }
        }
        // list 和 file 方式是兼容老的数据源参数，list 默认表示从七牛进行列举，file 表示从本地读取文件
        if ("list".equals(source)) source = "qiniu";
        else if ("file".equals(source)) source = "local";
        if (!source.matches("(local|qiniu|tencent)")) {
            throw new IOException("please set the \"source\" conform to regex: (local|qiniu|tencent)");
        }
    }

    private void setBaseFieldsFilter() throws Exception {
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
        String[] dateScale = splitDateScale(entryParam.getValue("f-date-scale", null));
        long putTimeMin = checkedDatetime(dateScale[0]);
        long putTimeMax = checkedDatetime(dateScale[1]);
        if (putTimeMax != 0 && putTimeMax <= putTimeMin ) {
            throw new IOException("please set date scale to make first as start date, next as end date, <date1> " +
                    "should earlier then <date2>.");
        }
        String type = entryParam.getValue("f-type", null);
        String status = entryParam.getValue("f-status", null);
        if (type != null) type = checked(type, "f-type", "[01]");
        if (status != null) status = checked(status, "f-status", "[01]");

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
        baseFieldsFilter = new BaseFieldsFilter(keyPrefixList, keySuffixList, keyInnerList, keyRegexList, mimeTypeList,
                putTimeMin, putTimeMax, type, status);
        baseFieldsFilter.setAntiConditions(antiKeyPrefixList, antiKeySuffixList, antiKeyInnerList, antiKeyRegexList,
                antiMimeTypeList);
    }

    private void setSeniorChecker() throws IOException {
        String checkType = entryParam.getValue("f-check", "");
        checkType = checked(checkType, "f-check", "(|mime)");
        String checkConfig = entryParam.getValue("f-check-config", "");
        String checkRewrite = entryParam.getValue("f-check-rewrite", "false");
        checkRewrite = checked(checkRewrite, "f-check-rewrite", "(true|false)");
        seniorChecker = new SeniorChecker(checkType, checkConfig, Boolean.valueOf(checkRewrite));
    }

    /**
     * 支持从路径方式上解析出 bucket，如果主动设置 bucket 则替换路径中的值
     * @throws IOException
     */
    private void setBucket() throws IOException {
        if (path.startsWith("qiniu://")) {
            bucket = path.substring(8);
            bucket = entryParam.getValue("bucket", bucket);
        } else if (path.startsWith("tencent://")) {
            bucket = path.substring(10);
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
        if (saveTotal == null) {
            if (source.matches("(qiniu|tencent)")) {
                if (process == null) {
                    saveTotal = "true";
                } else {
                    if (baseFieldsFilter.isValid() || seniorChecker.isValid()) saveTotal = "true";
                    else saveTotal = "false";
                }
            } else {
                if (process != null || baseFieldsFilter.isValid() || seniorChecker.isValid()) saveTotal = "false";
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
                            FileInfo markerFileInfo = new FileInfo();
                            markerFileInfo.key = jsonCfg.get("start").getAsString();
                            marker = OSSUtils.calcMarker(markerFileInfo);
                        } else if ("tencent".equals(source)) {
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

    private void setIndex(String indexName, String index, boolean check) throws IOException {
        if (check && indexMap.containsKey(indexName)) {
            throw new IOException("index: " + indexName + " is already used by \"" + indexMap.get(indexName)
                    + "-index=" + indexName + "\"");
        }
        if (indexName != null && !"-1".equals(indexName)) {
            if ("json".equals(parse) || "object".equals(parse)) {
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
        List<String> keys = LineUtils.fileInfoFields;
        List<String> indexList = splitItems(entryParam.getValue("indexes", ""));
        if (indexList.size() > 9) {
            throw new IOException("the file info's index length is too long.");
        } else {
            for (int i = 0; i < indexList.size(); i++) {
                setIndex(indexList.get(i), keys.get(i), true);
            }
        }

        if ("local".equals(source)) {
            setIndex(entryParam.getValue("url-index", null), "url", ProcessUtils.needUrl(process));
            setIndex(entryParam.getValue("newKey-index", null), "newKey", ProcessUtils.needNewKey(process));
            setIndex(entryParam.getValue("fops-index", null), "fops", ProcessUtils.needFops(process));
            setIndex(entryParam.getValue("persistentId-index", null), "pid", ProcessUtils.needPid(process));
            setIndex(entryParam.getValue("avinfo-index", null), "avinfo", ProcessUtils.needAvinfo(process));
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
                    if (baseFieldsFilter.checkMime() || seniorChecker.checkMime()) indexMap.put("mimeType", "mimeType");
                    if (baseFieldsFilter.checkPutTime()) indexMap.put("putTime", "putTime");
                    if (baseFieldsFilter.checkType()) indexMap.put("type", "type");
                    if (baseFieldsFilter.checkStatus()) indexMap.put("status", "status");
                } else {
                    for (String key : keys) {
                        indexMap.put(key, key);
                    }
                }
            }
            if (ProcessUtils.supportListSource(process)) {
                if (!indexMap.containsValue("key"))
                    throw new IOException("please check your indexes settings, miss a key index in first position.");
            } else if (process != null) {
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
                throw new IOException("f-" + name + " filter must get the " + key + "'s index in indexes settings.");
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

    public void setPath(String path) {
        this.path = path;
    }

    public void setBaseFieldsFilter(BaseFieldsFilter baseFieldsFilter) {
        this.baseFieldsFilter = baseFieldsFilter;
    }

    public void setSeniorChecker(SeniorChecker seniorChecker) {
        this.seniorChecker = seniorChecker;
    }

    public void setProcess(String process) {
        this.process = process;
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

    public String getPath() {
        return path;
    }

    public String getProcess() {
        return process;
    }

    public String getRmKeyPrefix() {
        return rmKeyPrefix;
    }

    public String getSource() {
        return source;
    }

    public BaseFieldsFilter getBaseFieldsFilter() {
        return baseFieldsFilter;
    }

    public SeniorChecker getSeniorChecker() {
        return seniorChecker;
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
