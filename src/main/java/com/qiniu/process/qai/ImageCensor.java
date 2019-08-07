package com.qiniu.process.qai;

import com.google.gson.JsonObject;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.RequestUtils;

import java.io.IOException;
import java.util.Map;

public class ImageCensor extends Base<Map<String, String>> {

    private String domain;
    private String protocol;
    private String urlIndex;
    private String suffixOrQuery;
    private boolean useQuery;
    private JsonObject paramsJson;
    private Configuration configuration;
    private CensorManager censorManager;

    public ImageCensor(String accesskey, String secretKey, Configuration configuration, String domain, String protocol,
                       String urlIndex, String suffixOrQuery, Scenes scenes)
            throws IOException {
        super("imagecensor", accesskey, secretKey, null);
        set(configuration, domain, protocol, urlIndex, suffixOrQuery, scenes);
        censorManager = new CensorManager(Auth.create(accesskey, secretKey), configuration.clone());
    }

    public ImageCensor(String accesskey, String secretKey, Configuration configuration, String domain, String protocol,
                       String urlIndex, String suffixOrQuery, Scenes scenes, String savePath, int saveIndex) throws IOException {
        super("imagecensor", accesskey, secretKey, null, savePath, saveIndex);
        set(configuration, domain, protocol, urlIndex, suffixOrQuery, scenes);
        censorManager = new CensorManager(Auth.create(accesskey, secretKey), configuration.clone());
    }

    public ImageCensor(String accesskey, String secretKey, Configuration configuration, String domain, String protocol,
                       String urlIndex, String suffixOrQuery, Scenes scenes, String savePath) throws IOException {
        this(accesskey, secretKey, configuration, domain, protocol, urlIndex, suffixOrQuery, scenes, savePath, 0);
    }

    private void set(Configuration configuration, String domain, String protocol, String urlIndex, String suffixOrQuery,
                     Scenes scenes) throws IOException {
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
        this.suffixOrQuery = suffixOrQuery == null ? "" : suffixOrQuery;
        useQuery = !"".equals(this.suffixOrQuery);
        this.paramsJson = new JsonObject();
        paramsJson.add("scenes", CensorManager.scenesMap.get(scenes));
    }

    public ImageCensor clone() throws CloneNotSupportedException {
        ImageCensor videoCensor = (ImageCensor)super.clone();
        videoCensor.censorManager = new CensorManager(Auth.create(authKey1, authKey2), configuration.clone());
        return videoCensor;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        String key = line.get("key");
        return (key == null ? "\t" : key + "\t") + line.get(urlIndex);
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String url = line.get(urlIndex);
        String key = line.get("key");
        if (url == null || "".equals(url)) {
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            url = protocol + "://" + domain + "/" + key.replace("\\?", "%3f") + suffixOrQuery;
            line.put(urlIndex, url);
            return key + "\t" + url + "\t" + censorManager.doImageCensor(url, paramsJson);
        } else if (useQuery) {
            url = url + suffixOrQuery;
            line.put(urlIndex, url);
        }
        return (key == null ? "\t" : key + "\t") + url + "\t" + censorManager.doImageCensor(url, paramsJson);
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
