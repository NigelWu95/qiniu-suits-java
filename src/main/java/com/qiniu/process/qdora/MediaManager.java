package com.qiniu.process.qdora;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.model.qdora.*;
import com.qiniu.util.Auth;
import com.qiniu.util.JsonConvertUtils;

public class MediaManager {

    final private Client client;
    final private String protocol;
    final private Auth srcAuth;

    public MediaManager() {
        this.client = new Client();
        this.protocol = "http";
        this.srcAuth = null;
    }

    public MediaManager(String protocol, Auth srcAuth) {
        this.client = new Client();
        this.protocol = "https".equals(protocol)? "https" : "http";
        this.srcAuth = srcAuth;
    }

    public Avinfo getAvinfo(String url) throws QiniuException {
        return getAvinfoByJson(getAvinfoJson(url)) ;
    }

    public Avinfo getAvinfo(String domain, String sourceKey) throws QiniuException {
        return getAvinfoByJson(getAvinfoJson(domain, sourceKey)) ;
    }

    public Avinfo getAvinfoByJson(String avinfoJson) throws QiniuException {
        try {
            JsonObject jsonObject = JsonConvertUtils.fromJson(avinfoJson, JsonObject.class);
            return getAvinfoByJson(jsonObject);
        } catch (JsonParseException e) {
            throw new QiniuException(e, e.getMessage());
        }
    }

    public Avinfo getAvinfoByJson(JsonObject avinfoJson) throws QiniuException {
        Avinfo avinfo = new Avinfo();
        try {
            avinfo.setFormat(JsonConvertUtils.fromJson(avinfoJson.getAsJsonObject("format"), Format.class));
            JsonElement element = avinfoJson.get("streams");
            JsonArray streams = element.getAsJsonArray();
            for (JsonElement stream : streams) {
                JsonElement typeElement = stream.getAsJsonObject().get("codec_type");
                String type = (typeElement == null || typeElement instanceof JsonNull) ? "" : typeElement.getAsString();
                if ("video".equals(type)) avinfo.setVideoStream(JsonConvertUtils.fromJson(stream, VideoStream.class));
                if ("audio".equals(type)) avinfo.setAudioStream(JsonConvertUtils.fromJson(stream, AudioStream.class));
            }
        } catch (JsonParseException e) {
            throw new QiniuException(e, e.getMessage());
        }
        return avinfo;
    }

    public JsonObject getAvinfoJson(String domain, String sourceKey) throws QiniuException {
        return getAvinfoJson(protocol + "://" + domain + "/" + sourceKey.split("\\?")[0]);
    }

    public JsonObject getAvinfoJson(String url) throws QiniuException {
        JsonParser jsonParser = new JsonParser();
        JsonObject avinfoJson = jsonParser.parse(getAvinfoBody(url)).getAsJsonObject();
        JsonElement jsonElement = avinfoJson.get("format");
        if (jsonElement == null || jsonElement instanceof JsonNull) {
            throw new QiniuException(null, "body error.");
        }
        return avinfoJson;
    }

    public String getAvinfoBody(String domain, String sourceKey) throws QiniuException {
        String url = protocol + "://" + domain + "/" + sourceKey.split("\\?")[0];
        return getAvinfoBody(url);
    }

    public String getAvinfoBody(String url) throws QiniuException {
        url = srcAuth != null ? srcAuth.privateDownloadUrl(url + "?avinfo") : url + "?avinfo";
        Response response = client.get(url);
        if (response.statusCode != 200) throw new QiniuException(response);
        String avinfo = response.bodyString();
        response.close();
        return avinfo;
    }

    public PfopResult getPfopResultByJson(String pfopResultJson) throws QiniuException {
        PfopResult pfopResult;
        try {
            pfopResult = JsonConvertUtils.fromJson(pfopResultJson, PfopResult.class);
        } catch (JsonParseException e) {
            throw new QiniuException(e, e.getMessage());
        }
        return pfopResult;
    }

    public PfopResult getPfopResultByJson(JsonObject pfopResultJson) throws QiniuException {
        PfopResult pfopResult;
        try {
            pfopResult = JsonConvertUtils.fromJson(pfopResultJson, PfopResult.class);
        } catch (JsonParseException e) {
            throw new QiniuException(e, e.getMessage());
        }
        return pfopResult;
    }

    public PfopResult getPfopResultById(String persistentId) throws QiniuException {
        JsonObject pfopResultJson = JsonConvertUtils.toJsonObject(getPfopResultBodyById(persistentId));
        JsonElement jsonElement = pfopResultJson.get("reqid");
        if (jsonElement == null || jsonElement instanceof JsonNull) {
            throw new QiniuException(null, "body error.");
        }
        return getPfopResultByJson(pfopResultJson);
    }

    public String getPfopResultBodyById(String persistentId) throws QiniuException {
        String url = "http://api.qiniu.com/status/get/prefop?id=" + persistentId;
        Response response = client.get(url);
        String pfopResult = response.bodyString();
        if (response.statusCode != 200) throw new QiniuException(response);
        response.close();
        return pfopResult;
    }
}
