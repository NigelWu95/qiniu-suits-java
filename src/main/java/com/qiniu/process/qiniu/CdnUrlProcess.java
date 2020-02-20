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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CdnUrlProcess extends Base<Map<String, String>> {

    private String protocol;
    private String domain;
    private String urlIndex;
    private boolean isDir;
//    private boolean prefetch;
    private List<String> batches;
    private List<Map<String, String>> lines;
//    private Configuration configuration;
    private CdnHelper cdnHelper;
    private ICdnApplier cdnApplier;

    public CdnUrlProcess(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                         String urlIndex, boolean isDir, boolean prefetch) throws IOException {
        super(prefetch ? "cdnprefetch" : "cdnrefresh", accessKey, secretKey, null);
        Auth auth = Auth.create(accessKey, secretKey);
        cdnHelper = new CdnHelper(auth, configuration);
        this.cdnApplier = prefetch ? urls -> {
            String[] urlArray = new String[urls.size()];
            return cdnHelper.prefetch(urls.toArray(urlArray));
        } : isDir ? urls -> {
            String[] urlArray = new String[urls.size()];
            return cdnHelper.refresh(null, urls.toArray(urlArray));
        } : urls -> {
            String[] urlArray = new String[urls.size()];
            return cdnHelper.refresh(urls.toArray(urlArray), null);
        };
        CloudApiUtils.checkQiniu(auth);
        set(configuration, protocol, domain, urlIndex, isDir);
//        this.prefetch = prefetch;
    }

    public CdnUrlProcess(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                         String urlIndex, boolean isDir, boolean prefetch, String savePath, int saveIndex) throws IOException {
        super(prefetch ? "cdnprefetch" : "cdnrefresh", accessKey, secretKey, null, savePath, saveIndex);
        this.batchSize = isDir ? 10 : 30;
        this.batches = new ArrayList<>(batchSize);
        this.lines = new ArrayList<>(batchSize);
        Auth auth = Auth.create(accessKey, secretKey);
        cdnHelper = new CdnHelper(auth, configuration);
        this.cdnApplier = prefetch ? urls -> {
            String[] urlArray = new String[urls.size()];
            return cdnHelper.prefetch(urls.toArray(urlArray));
        } : isDir ? urls -> {
            String[] urlArray = new String[urls.size()];
            return cdnHelper.refresh(null, urls.toArray(urlArray));
        } : urls -> {
            String[] urlArray = new String[urls.size()];
            return cdnHelper.refresh(urls.toArray(urlArray), null);
        };
        CloudApiUtils.checkQiniu(auth);
        set(configuration, protocol, domain, urlIndex, isDir);
//        this.prefetch = prefetch;
        this.fileSaveMapper.preAddWriter("invalid");
    }

    public void set(Configuration configuration, String protocol, String domain, String urlIndex, boolean isDir) throws IOException {
//        this.configuration = configuration;
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
        this.isDir = isDir;
    }

    public CdnUrlProcess(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                         String urlIndex, boolean isDir, boolean prefetch, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, protocol, domain, urlIndex, isDir, prefetch, savePath, 0);
    }

    @Override
    public CdnUrlProcess clone() throws CloneNotSupportedException {
//        CdnUrlProcess cdnUrlProcess = (CdnUrlProcess)super.clone();
//        cdnUrlProcess.lines = new ArrayList<>(batchSize);
//        CdnHelper cdnHelper = new CdnHelper(Auth.create(accessId, secretKey), configuration);
//        cdnUrlProcess.cdnApplier = prefetch ? cdnHelper::prefetch : isDir ?
//                dirs -> cdnHelper.refresh(null, dirs) : urls -> cdnHelper.refresh(urls, null);
//        if (cdnUrlProcess.fileSaveMapper != null) {
//            cdnUrlProcess.fileSaveMapper.preAddWriter("invalid");
//        }
//        return cdnUrlProcess;
        if (!autoIncrease) saveIndex.addAndGet(1);
        return this;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return domain == null ? line.get(urlIndex) : line.get("key");
    }

    @Override
    protected synchronized List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) throws IOException {
        batches.clear();
        lines.clear();
        if (domain == null) {
            for (Map<String, String> line : processList) {
                lines.add(line);
                batches.add(line.get(urlIndex));
            }
        } else {
            String key;
            for (Map<String, String> line : processList) {
                key = line.get("key");
                if (key == null) {
                    fileSaveMapper.writeError("key and url are not exist or empty in " + line, false);
                } else {
                    lines.add(line);
                    batches.add(String.join("", protocol, "://", domain, "/",
                            key.replace("\\?", "%3f")));
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
            JsonObject tasks = refreshResult.get("taskIds").getAsJsonObject();
            fileSaveMapper.writeSuccess(tasks.entrySet().stream().map(entry -> String.join("\t", entry.getKey(),
                    entry.getValue().getAsString())).collect(Collectors.joining("\n")), false);
        } else {
            JsonArray jsonArray = new JsonArray();
            JsonElement jsonElement = isDir ? refreshResult.get("invalidDirs") : refreshResult.get("invalidUrls");
            if (!(jsonElement instanceof JsonNull)) jsonArray = jsonElement.getAsJsonArray();
            if (jsonArray.size() > 0) {
                StringBuilder builder = new StringBuilder(jsonArray.get(0).getAsString());
                for (int i = 1; i < jsonArray.size(); i++) {
                    builder.append("\n").append(jsonArray.get(i).getAsString());
                }
                fileSaveMapper.writeToKey("invalid", String.join("\t", builder,
                        String.valueOf(code), refreshResult.get("error").getAsString()), false);
            }
        }
        refreshResult = null;
        return null;
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String url;
        if (domain == null) {
            url = line.get(urlIndex);
            return String.join("\t", url, HttpRespUtils.getResult(cdnApplier.apply(new ArrayList<String>(){{ add(url); }})));
        } else {
            String key = line.get("key");
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            url = String.join("", protocol, "://", domain, "/", key.replace("\\?", "%3f"));
            return String.join("\t", key, HttpRespUtils.getResult(cdnApplier.apply(new ArrayList<String>(){{ add(url); }})));
        }
    }

    @Override
    public void closeResource() {
        if (saveIndex == null || saveIndex.get() <= index) {
            super.closeResource();
            protocol = null;
            domain = null;
            urlIndex = null;
            batches = null;
            lines = null;
            cdnHelper = null;
            cdnApplier = null;
        } else {
            saveIndex.decrementAndGet();
        }
    }
}
