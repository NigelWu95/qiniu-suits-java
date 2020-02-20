package com.qiniu.process.qiniu;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.interfaces.ICdnApplier;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CdnUrlQuery extends Base<Map<String, String>> {

    private String protocol;
    private String domain;
    private String urlIndex;
    private boolean prefetch;
    private String[] batches;
    private boolean hasOption;
    private int pageNo;
    private int pageSize;
    private String startTime;
    private String endTime;
    private Configuration configuration;
    private List<Map<String, String>> lines;
    private CdnHelper cdnHelper;
    private ICdnApplier cdnApplier;

    public CdnUrlQuery(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                       String urlIndex, boolean prefetch) throws IOException {
        super(prefetch ? "prefetchquery" : "refreshquery", accessKey, secretKey, null);
        Auth auth = Auth.create(accessKey, secretKey);
        cdnHelper = new CdnHelper(auth, configuration);
        this.cdnApplier = prefetch ? cdnHelper::queryPrefetch : cdnHelper::queryRefresh;
        CloudApiUtils.checkQiniu(auth);
        set(configuration, protocol, domain, urlIndex, prefetch);
    }

    public CdnUrlQuery(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                       String urlIndex, boolean prefetch, String savePath, int saveIndex) throws IOException {
        super(prefetch ? "prefetchquery" : "refreshquery", accessKey, secretKey, null, savePath, saveIndex);
        this.batchSize = 30;
        this.batches = new String[30];
        this.lines = new ArrayList<>();
        Auth auth = Auth.create(accessKey, secretKey);
        cdnHelper = new CdnHelper(auth, configuration);
        this.cdnApplier = prefetch ? cdnHelper::queryPrefetch : cdnHelper::queryRefresh;
        CloudApiUtils.checkQiniu(auth);
        set(configuration, protocol, domain, urlIndex, prefetch);
        this.fileSaveMapper.preAddWriter("processing");
    }

    public CdnUrlQuery(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                       String urlIndex, boolean prefetch, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, protocol, domain, urlIndex, prefetch, savePath, 0);
    }

    public void set(Configuration configuration, String protocol, String domain, String urlIndex, boolean prefetch) throws IOException {
        this.configuration = configuration;
        if (domain == null || "".equals(domain)) {
            if (urlIndex == null || "".equals(urlIndex)) {
                throw new IOException("please set one of domain and url-index.");
            } else {
                this.urlIndex = urlIndex;
            }
        } else {
            this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            RequestUtils.lookUpFirstIpFromHost(domain);
            this.domain = domain;
            this.urlIndex = "url";
        }
        this.prefetch = prefetch;
    }

    public void setQueryOptions(int pageNo, int pageSize, String startTime, String endTime) {
        this.pageNo = pageNo;
        this.pageSize = pageSize;
        this.startTime = startTime;
        this.endTime = endTime;
        this.hasOption = true;
        this.cdnApplier = prefetch ? urls -> {
            JsonArray urlArray = new JsonArray();
            for (String url : urls) urlArray.add(url);
            return cdnHelper.queryPrefetch(urlArray, pageNo, pageSize, startTime, endTime);
        } : urls -> {
            JsonArray urlArray = new JsonArray();
            for (String url : urls) urlArray.add(url);
            return cdnHelper.queryRefresh(urlArray, pageNo, pageSize, startTime, endTime);
        };
    }

    @Override
    public CdnUrlQuery clone() throws CloneNotSupportedException {
        CdnUrlQuery cdnUrlQuery = (CdnUrlQuery)super.clone();
        cdnUrlQuery.lines = new ArrayList<>();
        cdnUrlQuery.cdnHelper = new CdnHelper(Auth.create(accessId, secretKey), configuration);
        if (hasOption) {
            cdnUrlQuery.cdnApplier = prefetch ? urls -> {
                JsonArray urlArray = new JsonArray();
                for (String url : urls) urlArray.add(url);
                return cdnUrlQuery.cdnHelper.queryPrefetch(urlArray, pageNo, pageSize, startTime, endTime);
            } : urls -> {
                JsonArray urlArray = new JsonArray();
                for (String url : urls) urlArray.add(url);
                return cdnUrlQuery.cdnHelper.queryRefresh(urlArray, pageNo, pageSize, startTime, endTime);
            };
        } else {
            cdnUrlQuery.cdnApplier = prefetch ? cdnUrlQuery.cdnHelper::queryPrefetch : cdnUrlQuery.cdnHelper::queryRefresh;
        }
        if (cdnUrlQuery.fileSaveMapper != null) {
            cdnUrlQuery.fileSaveMapper.preAddWriter("processing");
        }
        return cdnUrlQuery;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return domain == null ? line.get(urlIndex) : line.get("key");
    }

    @Override
    protected synchronized List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) throws IOException {
        Arrays.fill(batches, null);
        lines.clear();
        Map<String, String> line;
        if (domain == null) {
            for (int i = 0; i < processList.size(); i++) {
                line = processList.get(i);
                lines.add(line);
                batches[i] = line.get(urlIndex);
            }
        } else {
            String key;
            for (int i = 0; i < processList.size(); i++) {
                line = processList.get(i);
                key = line.get("key");
                if (key == null) {
                    fileSaveMapper.writeError("key and url are not exist or empty in " + line, false);
                } else {
                    lines.add(line);
                    batches[i] = String.join("", protocol, "://", domain, "/",
                            key.replace("\\?", "%3f"));
                }
            }
        }
        return lines;
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws IOException {
        return HttpRespUtils.getResult(cdnApplier.apply(batches));
    }

    @Override
    protected List<Map<String, String>> parseBatchResult(List<Map<String, String>> processList, String result) throws Exception {
        if (result == null || "".equals(result)) throw new IOException("not valid refresh response.");
        JsonObject refreshResult = JsonUtils.toJsonObject(result);
        int code = refreshResult.get("code").getAsInt();
        if (code == 200) {
            JsonArray jsonArray = new JsonArray();
            JsonElement jsonElement = refreshResult.get("items");
            if (!(jsonElement instanceof JsonNull)) jsonArray = jsonElement.getAsJsonArray();
            if (jsonArray.size() > 0) {
                JsonObject item;
                String state;
                for (int i = 0; i < jsonArray.size(); i++) {
                    item = jsonArray.get(i).getAsJsonObject();
                    state = item.get("state").getAsString();
                    if ("success".equals(state)) {
                        fileSaveMapper.writeSuccess(item.toString(), false);
                    } else if ("processing".equals(state)) {
                        fileSaveMapper.writeToKey("processing", item.toString(), false);
                    } else {
                        fileSaveMapper.writeError(item.toString(), false);
                    }
                }
            }
        } else {
            fileSaveMapper.writeError(String.join("\t", processList.stream().map(this::resultInfo)
                    .collect(Collectors.joining("\n")), String.valueOf(code),
                    refreshResult.get("error").getAsString()), false);
        }
        refreshResult = null;
        return null;
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String url;
        if (domain == null) {
            url = line.get(urlIndex);
            String[] urls = new String[]{url};
            return String.join("\t", url, HttpRespUtils.getResult(cdnApplier.apply(urls)));
        } else {
            String key = line.get("key");
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            url = String.join("", protocol, "://", domain, "/", key.replace("\\?", "%3f"));
            String[] urls = new String[]{url};
            return String.join("\t", key, HttpRespUtils.getResult(cdnApplier.apply(urls)));
        }
    }

    @Override
    public void closeResource() {
        super.closeResource();
        protocol = null;
        domain = null;
        urlIndex = null;
        batches = null;
        lines = null;
        cdnHelper = null;
        cdnApplier = null;
        saveIndex = null;
    }
}
