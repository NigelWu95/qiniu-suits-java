package com.qiniu.process.qdora;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.model.qdora.*;
import com.qiniu.storage.Configuration;
import com.qiniu.util.JsonUtils;

import java.io.IOException;

public class MediaManager {

    private Client client;
    private String protocol;

    public MediaManager() {
        this.protocol = "http";
    }

    public MediaManager(String protocol) {
        this.client = new Client();
        this.protocol = "https".equals(protocol)? "https" : "http";
    }

    public MediaManager(Configuration configuration) {
        this.client = new Client(configuration);
        this.protocol = "http";
    }

    public MediaManager(Configuration configuration, String protocol) {
        this(configuration);
        this.protocol = "https".equals(protocol)? "https" : "http";
    }

    public Avinfo getAvinfo(String url) throws IOException {
        return getAvinfoByJson(getAvinfoJson(url)) ;
    }

    public Avinfo getAvinfo(String domain, String sourceKey) throws IOException {
        return getAvinfoByJson(getAvinfoJson(domain, sourceKey)) ;
    }

    public Avinfo getAvinfoByJson(JsonObject avinfoJson) throws IOException {
        Avinfo avinfo = new Avinfo();
        if (!avinfoJson.has("format") || !avinfoJson.has("streams"))
            throw new IOException(avinfoJson + " may be not a invalid avinfo string.");
        avinfo.setFormat(JsonUtils.fromJson(avinfoJson.getAsJsonObject("format"), Format.class));
        JsonElement element = avinfoJson.get("streams");
        JsonArray streams = element.getAsJsonArray();
        for (JsonElement stream : streams) {
            JsonElement typeElement = stream.getAsJsonObject().get("codec_type");
            String type = (typeElement == null || typeElement instanceof JsonNull) ? "" : typeElement.getAsString();
            if ("video".equals(type)) avinfo.setVideoStream(JsonUtils.fromJson(stream, VideoStream.class));
            if ("audio".equals(type)) avinfo.setAudioStream(JsonUtils.fromJson(stream, AudioStream.class));
        }
        return avinfo;
    }

    public JsonObject getAvinfoJson(String domain, String sourceKey) throws IOException {
        return getAvinfoJson(String.join("", protocol, "://", domain, "/", sourceKey.split("\\?")[0]));
    }

    public JsonObject getAvinfoJson(String url) throws IOException {
        JsonParser jsonParser = new JsonParser();
        JsonObject avinfoJson = jsonParser.parse(getAvinfoBody(url)).getAsJsonObject();
        JsonElement jsonElement = avinfoJson.get("format");
        if (jsonElement == null || jsonElement instanceof JsonNull) {
            throw new IOException("body error: " + jsonElement);
        }
        return avinfoJson;
    }

    public String getAvinfoBody(String domain, String sourceKey) throws QiniuException {
        String url = String.join("", protocol, "://", domain, "/", sourceKey.split("\\?")[0]);
        return getAvinfoBody(url);
    }

    public String getAvinfoBody(String url) throws QiniuException {
        if (client == null) this.client = new Client();
        Response response = client.get(String.join("?", url, "avinfo"));
        String avinfo = response.bodyString();
        if (response.statusCode != 200 || avinfo.isEmpty()) throw new QiniuException(response);
        response.close();
        return avinfo;
    }

    public String getPfopResultBodyById(String persistentId) throws QiniuException {
        String url = String.join("", protocol, "://api.qiniu.com/status/get/prefop?id=", persistentId);
        if (client == null) this.client = new Client();
        Response response = client.get(url);
        String pfopResult = response.bodyString();
        if (response.statusCode != 200 || pfopResult.isEmpty()) throw new QiniuException(response);
        response.close();
        return pfopResult;
    }
}
