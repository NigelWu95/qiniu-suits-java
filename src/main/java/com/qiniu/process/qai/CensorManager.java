package com.qiniu.process.qai;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.JsonUtils;
import com.qiniu.util.StringMap;

import java.io.IOException;

public class CensorManager {

    private Auth auth;
    private Client client;
    private JsonObject bodyJson;
    private JsonObject uriJson;
    private JsonObject paramsJson;
    private JsonObject saverJson;
    private JsonObject pictureCutJson;
    private StringMap headers;
    private static String imageCensorUrl = "http://ai.qiniuapi.com/v3/image/censor";
    private static String videoCensorUrl = "http://ai.qiniuapi.com/v3/video/censor";

    public static JsonArray getScenes(String[] scenes) throws IOException {
        JsonArray scenesJsonArray = new JsonArray();
        for (String scene : scenes) {
            try {
                Scenes.valueOf(scene);
            } catch (IllegalArgumentException e) {
                throw new IOException(e.getMessage() + ", error scene: \"" + scene + "\"");
            }
            scenesJsonArray.add(scene);
        }
        return scenesJsonArray;
    }

    public CensorManager(Auth auth, Configuration configuration) {
        this.auth = auth;
        this.client = configuration == null ? new Client() : new Client(configuration);
        this.bodyJson = new JsonObject();
        this.paramsJson = new JsonObject();
        this.uriJson = new JsonObject();
        this.saverJson = new JsonObject();
        this.pictureCutJson = new JsonObject();
        this.headers = new StringMap();
    }

    public CensorManager(Auth auth) {
        this(auth, null);
    }

    synchronized public String doImageCensor(String url, String[] scenes) throws IOException {
        uriJson.addProperty("uri", url);
        bodyJson.add("data", uriJson);
        paramsJson.add("scenes", getScenes(scenes));
        bodyJson.add("params", paramsJson);
        byte[] body = bodyJson.toString().getBytes();
        jsonClear();
        String token = "Qiniu " + auth.signRequestV2(imageCensorUrl, "POST", body, "application/json");
        headers.put("Authorization", token);
        Response response = client.post(imageCensorUrl, body, headers, Client.JsonMime);
        String result = response.bodyString();
        if (response.statusCode != 200 || result.isEmpty()) throw new QiniuException(response);
        response.close();
        return result;
    }

    synchronized public String doVideoCensor(String url, String[] scenes, int interval, String saverBucket, String saverPrefix,
                                             String hookUrl) throws IOException {
        uriJson.addProperty("uri", url);
        bodyJson.add("data", uriJson);
        paramsJson.add("scenes", getScenes(scenes));
        if ((saverBucket != null && !"".equals(saverBucket)) || (saverPrefix != null && !"".equals(saverPrefix))) {
            saverJson.addProperty("bucket", saverBucket);
            saverJson.addProperty("prefix", saverPrefix);
            paramsJson.add("saver", saverJson);
        }
        if (interval > 0) {
            pictureCutJson.addProperty("interval_msecs", interval);
            paramsJson.add("cut_param", pictureCutJson);
        }
        if (hookUrl != null && !"".equals(hookUrl)) {
            paramsJson.addProperty("hook_url", hookUrl);
        }
        bodyJson.add("params", paramsJson);
        byte[] body = bodyJson.toString().getBytes();
        jsonClear();
        String token = "Qiniu " + auth.signRequestV2(videoCensorUrl, "POST", body, "application/json");
        headers.put("Authorization", token);
        Response response = client.post(videoCensorUrl, body, headers, Client.JsonMime);
        String result = response.bodyString();
        if (response.statusCode != 200 || result.isEmpty()) throw new QiniuException(response);
        response.close();
        return result;
    }

    synchronized private String doCensor(String requestUrl, String url, JsonObject paramsJson) throws QiniuException {
        uriJson.addProperty("uri", url);
        bodyJson.add("data", uriJson);
        bodyJson.add("params", paramsJson);
        byte[] body = bodyJson.toString().getBytes();
        bodyJson.remove("data");
        bodyJson.remove("params");
        String token = "Qiniu " + auth.signRequestV2(requestUrl, "POST", body, "application/json");
        headers.put("Authorization", token);
        Response response = client.post(requestUrl, body, headers, Client.JsonMime);
        String result = response.bodyString();
        if (response.statusCode != 200 || result.isEmpty()) throw new QiniuException(response);
        response.close();
        return result;
    }

    public String doImageCensor(String url, JsonObject paramsJson) throws QiniuException {
        return doCensor(imageCensorUrl, url, paramsJson);
    }

    public String doVideoCensor(String url, JsonObject paramsJson) throws QiniuException {
        return doCensor(videoCensorUrl, url, paramsJson);
    }

    private void jsonClear() {
        bodyJson.remove("data");
        bodyJson.remove("params");
        paramsJson.remove("saver");
        paramsJson.remove("cut_param");
        paramsJson.remove("hook_url");
    }

    public String censorString(String jobId) throws QiniuException {
        String queryUrl = "http://ai.qiniuapi.com/v3/jobs/video/" + jobId;
        String token = "Qiniu " + auth.signRequestV2(queryUrl, "GET", null, null);
        headers.put("Authorization", token);
        Response response = client.get(queryUrl, headers);
        String result = response.bodyString();
        if (response.statusCode != 200 || result.isEmpty()) throw new QiniuException(response);
        response.close();
        return result;
    }

    public CensorResult censorResult(String jobId) throws QiniuException {
        return JsonUtils.fromJson(censorString(jobId), CensorResult.class);
    }
}
