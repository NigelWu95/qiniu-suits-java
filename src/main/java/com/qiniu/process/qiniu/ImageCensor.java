package com.qiniu.process.qiniu;

import com.google.gson.JsonObject;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.CloudApiUtils;
import com.qiniu.util.RequestUtils;

import java.io.IOException;
import java.util.Map;

public class ImageCensor extends Base<Map<String, String>> {

    private String protocol;
    private String domain;
    private String urlIndex;
    private String suffixOrQuery;
    private JsonObject paramsJson;
    private Configuration configuration;
    private CensorManager censorManager;

    public ImageCensor(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                       String urlIndex, String suffixOrQuery, String[] scenes) throws IOException {
        super("imagecensor", accessKey, secretKey, null);
        set(configuration, protocol, domain, urlIndex, suffixOrQuery, scenes);
        Auth auth = Auth.create(accessKey, secretKey);
        CloudApiUtils.checkQiniu(auth);
        censorManager = new CensorManager(auth, configuration);
    }

    public ImageCensor(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                       String urlIndex, String suffixOrQuery, String[] scenes, String savePath, int saveIndex) throws IOException {
        super("imagecensor", accessKey, secretKey, null, savePath, saveIndex);
        set(configuration, protocol, domain, urlIndex, suffixOrQuery, scenes);
        Auth auth = Auth.create(accessKey, secretKey);
        CloudApiUtils.checkQiniu(auth);
        censorManager = new CensorManager(auth, configuration);
    }

    public ImageCensor(String accesskey, String secretKey, Configuration configuration, String protocol, String domain,
                       String urlIndex, String suffixOrQuery, String[] scenes, String savePath) throws IOException {
        this(accesskey, secretKey, configuration, protocol, domain, urlIndex, suffixOrQuery, scenes, savePath, 0);
    }

    private void set(Configuration configuration, String protocol, String domain, String urlIndex, String suffixOrQuery,
                     String[] scenes) throws IOException {
        this.configuration = configuration;
        if (domain == null || "".equals(domain)) {
            if (urlIndex == null || "".equals(urlIndex)) {
                throw new IOException("please set one of domain and url-index.");
            } else {
                this.urlIndex = urlIndex;
            }
            this.domain = null;
        } else {
            this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
            RequestUtils.lookUpFirstIpFromHost(domain);
            this.domain = domain;
            this.urlIndex = "url";
        }
        this.suffixOrQuery = suffixOrQuery == null ? "" : suffixOrQuery;
        this.paramsJson = new JsonObject();
        paramsJson.add("scenes", CensorManager.getScenes(scenes));
    }

    @Override
    public ImageCensor clone() throws CloneNotSupportedException {
        ImageCensor videoCensor = (ImageCensor)super.clone();
        videoCensor.censorManager = new CensorManager(Auth.create(accessId, secretKey), configuration);
        return videoCensor;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return domain == null ? line.get(urlIndex) : line.get("key");
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String url;
        if (domain == null) {
            url = line.get(urlIndex);
            return String.join("\t", url, censorManager.doImageCensor(url + suffixOrQuery, paramsJson));
        } else {
            String key = line.get("key");
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            url = String.join("", protocol, "://", domain, "/", key.replace("\\?", "%3f"), suffixOrQuery);
            return String.join("\t", key, censorManager.doImageCensor(url, paramsJson));
        }
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
