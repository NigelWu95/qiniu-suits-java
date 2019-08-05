package com.qiniu.process.qai;

import com.google.gson.JsonObject;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.RequestUtils;

import java.io.IOException;
import java.util.Map;

public class VideoCensor extends Base<Map<String, String>> {

    private String domain;
    private String protocol;
    private String urlIndex;
    private JsonObject paramsJson;
    private Configuration configuration;
    private CensorManager censorManager;

    public VideoCensor(String accesskey, String secretKey, Configuration configuration, String domain, String protocol,
                       String urlIndex, Scenes scenes, int interval, String saverBucket, String saverPrefix, String hookUrl)
            throws IOException {
        super("imagecensor", accesskey, secretKey, null);
        set(configuration, domain, protocol, urlIndex, scenes, interval, saverBucket, saverPrefix, hookUrl);
        censorManager = new CensorManager(Auth.create(accesskey, secretKey), configuration.clone());
    }

    public VideoCensor(String accesskey, String secretKey, Configuration configuration, String domain, String protocol,
                       String urlIndex, Scenes scenes, int interval, String saverBucket, String saverPrefix, String hookUrl,
                       String savePath, int saveIndex) throws IOException {
        super("imagecensor", accesskey, secretKey, null, savePath, saveIndex);
        set(configuration, domain, protocol, urlIndex, scenes, interval, saverBucket, saverPrefix, hookUrl);
        censorManager = new CensorManager(Auth.create(accesskey, secretKey), configuration.clone());
    }

    public VideoCensor(String accesskey, String secretKey, Configuration configuration, String domain, String protocol,
                       String urlIndex, Scenes scenes, int interval, String saverBucket, String saverPrefix, String hookUrl,
                       String savePath)
            throws IOException {
        this(accesskey, secretKey, configuration, domain, protocol, urlIndex, scenes, interval, saverBucket, saverPrefix,
                hookUrl, savePath, 0);
    }

    private void set(Configuration configuration, String domain, String protocol, String urlIndex, Scenes scenes,
                     int interval, String saverBucket, String saverPrefix, String hookUrl) throws IOException {
        this.configuration = configuration;
        if (urlIndex == null || "".equals(urlIndex)) {
            this.urlIndex = "url";
            if (domain == null || "".equals(domain)) {
                throw new IOException("please set one of domain and url-index.");
            } else {
                RequestUtils.lookUpFirstIpFromHost(domain);
                this.domain = domain;
                this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            }
        } else {
            this.urlIndex = urlIndex;
        }
        this.paramsJson = new JsonObject();
        paramsJson.add("scenes", CensorManager.scenesMap.get(scenes));
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

    public void updateDomain(String domain) {
        this.domain = domain;
    }

    public void updateProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void updateUrlIndex(String urlIndex) {
        this.urlIndex = urlIndex;
    }

    public void updateCensorParams(JsonObject paramsJson) {
        this.paramsJson = paramsJson;
    }

    public VideoCensor clone() throws CloneNotSupportedException {
        VideoCensor videoCensor = (VideoCensor)super.clone();
        videoCensor.censorManager = new CensorManager(Auth.create(authKey1, authKey2), configuration.clone());
        return videoCensor;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get(urlIndex);
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String url = line.get(urlIndex);
        if (url == null || "".equals(url)) {
            String key = line.get("key");
            if (key == null) throw new IOException("no key in " + line);
            url = protocol + "://" + domain + "/" + key.replaceAll("\\?", "%3f");
            line.put(urlIndex, url);
        }
        return censorManager.doVideoCensor(url, paramsJson);
    }

    @Override
    public void closeResource() {
        super.closeResource();
        domain = null;
        protocol = null;
        urlIndex = null;
        paramsJson = null;
        configuration = null;
        censorManager = null;
    }
}
