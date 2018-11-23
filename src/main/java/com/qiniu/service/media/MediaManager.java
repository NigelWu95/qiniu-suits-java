package com.qiniu.service.media;

import com.google.gson.*;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.model.media.*;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.RequestUtils;

import java.net.UnknownHostException;

public class MediaManager {

    private Client client;
    private Avinfo avinfo;
    private JsonObject avinfoJson;

    public MediaManager() {
        this.client = new Client();
        this.avinfo = new Avinfo();
    }

    public String getCurrentAvinfoJson() {
        return JsonConvertUtils.toJson(avinfoJson);
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
        return getAvinfoByUrl(domain, key.toString().substring(0, key.length() - 1));
    }

    public Avinfo getAvinfoByUrl(String domain, String sourceKey) throws QiniuException {

        try {
            RequestUtils.checkHost(domain);
        } catch (UnknownHostException e) {
            throw new QiniuException(e);
        }
        String url = "http://" + domain + "/" + sourceKey.split("\\?")[0];
        Response response = client.get(url + "?avinfo");
        avinfoJson = JsonConvertUtils.toJsonObject(response.bodyString());
        response.close();
        JsonElement jsonElement = avinfoJson.get("format");
        if (jsonElement == null || jsonElement instanceof JsonNull) {
            throw new QiniuException(response);
        }
        return getAvinfoByJson(avinfoJson) ;
    }

    public Avinfo getAvinfoByJson(String avinfoJson) throws QiniuException {

        return getAvinfoByJson(JsonConvertUtils.toJsonObject(avinfoJson));
    }

    public Avinfo getAvinfoByJson(JsonObject avinfoJson) throws QiniuException {

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
}
