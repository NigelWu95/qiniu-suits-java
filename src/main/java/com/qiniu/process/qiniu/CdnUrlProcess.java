package com.qiniu.process.qiniu;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.qiniu.interfaces.ICdnApplier;
import com.qiniu.process.Base;
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
    private ICdnApplier cdnApplier;

    public CdnUrlProcess(String accessKey, String secretKey, String protocol, String domain, String urlIndex, boolean isDir,
                         boolean prefetch) throws IOException {
        super(prefetch ? "cdnprefetch" : "cdnrefresh", accessKey, secretKey, null);
        Auth auth = Auth.create(accessKey, secretKey);
        CdnHelper cdnHelper = new CdnHelper(auth);
        this.cdnApplier = prefetch ? cdnHelper::prefetch : isDir ?
                dirs -> cdnHelper.refresh(null, dirs) : urls -> cdnHelper.refresh(urls, null);
        CloudApiUtils.checkQiniu(auth);
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = "url";
            if (domain == null || "".equals(domain)) {
                throw new IOException("please set one of domain and url-index.");
            } else {
                this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
                RequestUtils.lookUpFirstIpFromHost(domain);
                this.domain = domain;
            }
        } else {
            this.urlIndex = urlIndex;
        }
        this.isDir = isDir;
//        this.prefetch = prefetch;
    }

    public CdnUrlProcess(String accessKey, String secretKey, String protocol, String domain, String urlIndex, boolean isDir,
                         boolean prefetch, String savePath, int saveIndex) throws IOException {
        super(prefetch ? "cdnprefetch" : "cdnrefresh", accessKey, secretKey, null, savePath, saveIndex);
        this.batchSize = 30;
        this.batches = new ArrayList<>(30);
        this.lines = new ArrayList<>();
        Auth auth = Auth.create(accessKey, secretKey);
        CdnHelper cdnHelper = new CdnHelper(auth);
        this.cdnApplier = prefetch ? cdnHelper::prefetch : isDir ?
                dirs -> cdnHelper.refresh(null, dirs) : urls -> cdnHelper.refresh(urls, null);
        CloudApiUtils.checkQiniu(auth);
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = "url";
            if (domain == null || "".equals(domain)) {
                throw new IOException("please set one of domain and url-index.");
            } else {
                this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
                RequestUtils.lookUpFirstIpFromHost(domain);
                this.domain = domain;
            }
        } else {
            this.urlIndex = urlIndex;
        }
        this.isDir = isDir;
//        this.prefetch = prefetch;
        this.fileSaveMapper.preAddWriter("invalid");
    }

    public CdnUrlProcess(String accessKey, String secretKey, String protocol, String domain, String urlIndex, boolean isDir,
                         boolean prefetch, String savePath) throws IOException {
        this(accessKey, secretKey, protocol, domain, urlIndex, isDir, prefetch, savePath, 0);
    }

    @Override
    public CdnUrlProcess clone() throws CloneNotSupportedException {
//        CdnUrlProcess cdnUrlProcess = (CdnUrlProcess)super.clone();
//        cdnUrlProcess.lines = new ArrayList<>();
//        CdnHelper cdnHelper = new CdnHelper(Auth.create(accessId, secretKey));
//        cdnUrlProcess.cdnApplier = prefetch ? cdnHelper::prefetch : isDir ?
//                dirs -> cdnHelper.refresh(null, dirs) : urls -> cdnHelper.refresh(urls, null);
//        if (cdnUrlProcess.fileSaveMapper != null) {
//            cdnUrlProcess.fileSaveMapper.preAddWriter("invalid");
//        }
//        return cdnUrlProcess;
        return this;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        String key = line.get("key");
        return key == null ? line.get(urlIndex) : String.join("\t", key, line.get(urlIndex));
    }

    @Override
    protected synchronized List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) throws IOException {
        batches.clear();
        lines.clear();
        String key;
        String url;
        for (Map<String, String> line : processList) {
            key = line.get("key");
            url = line.get(urlIndex);
            if (url == null || "".equals(url)) {
                if (key == null) {
                    fileSaveMapper.writeError("key and url are not exist or empty in " + line, false);
                } else {
                    url = String.join("", protocol, "://", domain, "/", key.replace("\\?", "%3f"));
                    line.put(urlIndex, url);
                    lines.add(line);
                    batches.add(url);
                }
            } else {
                lines.add(line);
                batches.add(url);
            }
        }
        return lines;
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws IOException {
        String[] urls = new String[batches.size()];
        return HttpRespUtils.getResult(cdnApplier.apply(batches.toArray(urls)));
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
        String key = line.get("key");
        String url = line.get(urlIndex);
        if (url == null || "".equals(url)) {
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            url = String.join("", protocol, "://", domain, "/", key.replace("\\?", "%3f"));
        }
        String[] urls = new String[]{url};
        return key == null ? String.join("\t", url, HttpRespUtils.getResult(cdnApplier.apply(urls))) :
                String.join("\t", key, url, HttpRespUtils.getResult(cdnApplier.apply(urls)));
    }

    @Override
    public void closeResource() {
        super.closeResource();
        protocol = null;
        domain = null;
        urlIndex = null;
        batches = null;
        lines = null;
        cdnApplier = null;
    }
}
