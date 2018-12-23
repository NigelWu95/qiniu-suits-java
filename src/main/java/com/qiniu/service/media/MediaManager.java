package com.qiniu.service.media;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.model.media.*;
import com.qiniu.util.Auth;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.RequestUtils;

import java.net.UnknownHostException;

public class MediaManager {

    private Client client;
    private String protocol;
    private Auth srcAuth;

    public MediaManager() {
        this.client = new Client();
    }

    public MediaManager(String protocol, Auth srcAuth) {
        this();
        this.protocol = protocol == null || "".equals(protocol) || !protocol.matches("(http|https)") ? "http" : protocol;;
        this.srcAuth = srcAuth;
    }

    public Avinfo getAvinfo(String url) throws QiniuException, UnknownHostException {

        String[] addr = url.split("/");
        if (addr.length < 3) throw new QiniuException(null, "not valid url.");
        String domain = addr[2];
        RequestUtils.checkHost(domain);
        StringBuilder key = new StringBuilder();
        for (int i = 3; i < addr.length; i++) {
            key.append(addr[i]).append("/");
        }
        return getAvinfo(domain, key.toString().substring(0, key.length() - 1));
    }

    public Avinfo getAvinfo(String domain, String sourceKey) throws QiniuException {

        return getAvinfoByJson(getAvinfoJson(domain, sourceKey)) ;
    }

    public Avinfo getAvinfoByJson(String avinfoJson) throws QiniuException {

        Avinfo avinfo = new Avinfo();
        try {
            JsonObject jsonObject = JsonConvertUtils.fromJson(avinfoJson, JsonObject.class);
            avinfo.setFormat(JsonConvertUtils.fromJson(jsonObject.getAsJsonObject("format"), Format.class));
            JsonElement element = jsonObject.get("streams");
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

        JsonParser jsonParser = new JsonParser();
        JsonObject avinfoJson = jsonParser.parse(getAvinfoBody(domain, sourceKey)).getAsJsonObject();
        JsonElement jsonElement = avinfoJson.get("format");
        if (jsonElement == null || jsonElement instanceof JsonNull) {
            throw new QiniuException(null, "body error.");
        }
        return avinfoJson;
    }

    public String getAvinfoBody(String domain, String sourceKey) throws QiniuException {

        try {
            RequestUtils.checkHost(domain);
        } catch (UnknownHostException e) {
            throw new QiniuException(e);
        }
        String url = protocol + "://" + domain + "/" + sourceKey.split("\\?")[0];
        return getAvinfoBody(url);
    }

    public String getAvinfoBody(String url) throws QiniuException {
        url = srcAuth != null ? srcAuth.privateDownloadUrl(url + "?avinfo") : url + "?avinfo";
        Response response = client.get(url);
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
        response.close();
        return pfopResult;
    }
}
