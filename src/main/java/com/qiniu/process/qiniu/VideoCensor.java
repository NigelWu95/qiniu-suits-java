package com.qiniu.process.qiniu;

import com.google.gson.JsonObject;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.CloudApiUtils;
import com.qiniu.util.JsonUtils;
import com.qiniu.util.RequestUtils;

import java.io.IOException;
import java.util.Map;

public class VideoCensor extends Base<Map<String, String>> {

    private String protocol;
    private String domain;
    private String urlIndex;
    private JsonObject paramsJson;
    private Configuration configuration;
    private CensorManager censorManager;

    public VideoCensor(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                       String urlIndex, String[] scenes, int interval, String saverBucket, String saverPrefix, String hookUrl)
            throws IOException {
        super("videocensor", accessKey, secretKey, null);
        set(configuration, protocol, domain, urlIndex, scenes, interval, saverBucket, saverPrefix, hookUrl);
        Auth auth = Auth.create(accessKey, secretKey);
        CloudApiUtils.checkQiniu(auth);
        censorManager = new CensorManager(auth, configuration.clone());
    }

    public VideoCensor(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                       String urlIndex, String[] scenes, int interval, String saverBucket, String saverPrefix, String hookUrl,
                       String savePath, int saveIndex) throws IOException {
        super("videocensor", accessKey, secretKey, null, savePath, saveIndex);
        set(configuration, protocol, domain, urlIndex, scenes, interval, saverBucket, saverPrefix, hookUrl);
        Auth auth = Auth.create(accessKey, secretKey);
        CloudApiUtils.checkQiniu(auth);
        censorManager = new CensorManager(auth, configuration.clone());
    }

    public VideoCensor(String accesskey, String secretKey, Configuration configuration, String protocol, String domain,
                       String urlIndex, String[] scenes, int interval, String saverBucket, String saverPrefix, String hookUrl,
                       String savePath)
            throws IOException {
        this(accesskey, secretKey, configuration, protocol, domain, urlIndex, scenes, interval, saverBucket, saverPrefix,
                hookUrl, savePath, 0);
    }

    private void set(Configuration configuration, String protocol, String domain, String urlIndex, String[] scenes,
                     int interval, String saverBucket, String saverPrefix, String hookUrl) throws IOException {
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
        this.paramsJson = new JsonObject();
        paramsJson.add("scenes", CensorManager.getScenes(scenes));
        if ((saverBucket != null && !"".equals(saverBucket)) || (saverPrefix != null && !"".equals(saverPrefix))) {
            JsonObject saverJson = new JsonObject();
            saverJson.addProperty("bucket", saverBucket);
            saverJson.addProperty("prefix", saverPrefix);
            paramsJson.add("saver", saverJson);
        }
        if (interval > 0) {
            if (interval > 60000 || interval < 1000) throw new IOException("invalid cut_param.interval_msecs.");
            JsonObject pictureCutJson = new JsonObject();
            pictureCutJson.addProperty("interval_msecs", interval);
            paramsJson.add("cut_param", pictureCutJson);
        }
        if (hookUrl != null && !"".equals(hookUrl)) {
            paramsJson.addProperty("hook_url", hookUrl);
        }
    }

    @Override
    public VideoCensor clone() throws CloneNotSupportedException {
        VideoCensor videoCensor = (VideoCensor)super.clone();
        videoCensor.censorManager = new CensorManager(Auth.create(accessId, secretKey), configuration.clone());
        return videoCensor;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        String key = line.get("key");
        return key == null ? line.get(urlIndex) : String.join("\t", key, line.get(urlIndex));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String url = line.get(urlIndex);
        String key = line.get("key");
        if (url == null || "".equals(url)) {
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            url = String.join("", protocol, "://", domain, "/", key.replace("\\?", "%3f"));
            line.put(urlIndex, url);
            return String.join("\t", key, url,
                    JsonUtils.toJsonObject(censorManager.doVideoCensor(url, paramsJson)).get("job").getAsString());
        }
        return key == null ? String.join("\t", url, JsonUtils.toJsonObject(censorManager.doVideoCensor(url, paramsJson)).get("job").getAsString()) :
                String.join("\t", key, url, JsonUtils.toJsonObject(censorManager.doVideoCensor(url, paramsJson)).get("job").getAsString());
    }

    @Override
    public void closeResource() {
        super.closeResource();
        protocol = null;
        domain = null;
        urlIndex = null;
        paramsJson = null;
        configuration = null;
        censorManager = null;
    }
}
