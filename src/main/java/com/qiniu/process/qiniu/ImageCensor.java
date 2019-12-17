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
    private boolean useQuery;
    private JsonObject paramsJson;
    private Configuration configuration;
    private CensorManager censorManager;

    public ImageCensor(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                       String urlIndex, String suffixOrQuery, String[] scenes) throws IOException {
        super("imagecensor", accessKey, secretKey, null);
        set(configuration, protocol, domain, urlIndex, suffixOrQuery, scenes);
        Auth auth = Auth.create(accessKey, secretKey);
        CloudApiUtils.checkQiniu(auth);
        censorManager = new CensorManager(auth, configuration.clone());
    }

    public ImageCensor(String accessKey, String secretKey, Configuration configuration, String protocol, String domain,
                       String urlIndex, String suffixOrQuery, String[] scenes, String savePath, int saveIndex) throws IOException {
        super("imagecensor", accessKey, secretKey, null, savePath, saveIndex);
        set(configuration, protocol, domain, urlIndex, suffixOrQuery, scenes);
        Auth auth = Auth.create(accessKey, secretKey);
        CloudApiUtils.checkQiniu(auth);
        censorManager = new CensorManager(auth, configuration.clone());
    }

    public ImageCensor(String accesskey, String secretKey, Configuration configuration, String protocol, String domain,
                       String urlIndex, String suffixOrQuery, String[] scenes, String savePath) throws IOException {
        this(accesskey, secretKey, configuration, protocol, domain, urlIndex, suffixOrQuery, scenes, savePath, 0);
    }

    private void set(Configuration configuration, String protocol, String domain, String urlIndex, String suffixOrQuery,
                     String[] scenes) throws IOException {
        this.configuration = configuration;
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
        this.suffixOrQuery = suffixOrQuery == null ? "" : suffixOrQuery;
        useQuery = !"".equals(this.suffixOrQuery);
        this.paramsJson = new JsonObject();
        paramsJson.add("scenes", CensorManager.getScenes(scenes));
    }

    @Override
    public ImageCensor clone() throws CloneNotSupportedException {
        ImageCensor videoCensor = (ImageCensor)super.clone();
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
            url = String.join("", protocol, "://", domain, "/",
                    key.replace("\\?", "%3f"), suffixOrQuery);
            line.put(urlIndex, url);
            return String.join("\t", key, url, censorManager.doImageCensor(url, paramsJson));
        } else if (useQuery) {
            url = String.join("", url, suffixOrQuery);
            line.put(urlIndex, url);
        }
        return key == null ? String.join("\t", url, censorManager.doImageCensor(url, paramsJson)) :
                String.join("\t", key, url, censorManager.doImageCensor(url, paramsJson));
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
