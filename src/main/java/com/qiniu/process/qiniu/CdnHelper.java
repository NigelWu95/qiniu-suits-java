package com.qiniu.process.qiniu;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.qiniu.common.Constants;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.Json;

import java.util.HashMap;
import java.util.Map;

public class CdnHelper {

    private final Auth auth;
    private final Client client;
    private static final String refreshUrl = "http://fusion.qiniuapi.com/v2/tune/refresh";
    private static final String prefetchUrl = "http://fusion.qiniuapi.com/v2/tune/prefetch";
    private static final String refreshQueryUrl = "http://fusion.qiniuapi.com/v2/tune/refresh/list";
    private static final String prefetchQueryUrl = "http://fusion.qiniuapi.com/v2/tune/prefetch/list";

    public CdnHelper(Auth auth, Configuration configuration) {
        this.auth = auth;
        this.client = configuration == null ? new Client() : new Client(configuration);
    }

    public Response refresh(String[] urls, String[] dirs) throws QiniuException {
        Map<String, String[]> req = new HashMap<>();
        if (urls != null) req.put("urls", urls);
        if (dirs != null) req.put("dirs", dirs);
        return UOperationForUrls(refreshUrl, req);
    }

    public Response queryRefresh(String[] urls) throws QiniuException {
        Map<String, String[]> req = new HashMap<>();
        req.put("urls", urls);
        return UOperationForUrls(refreshQueryUrl, req);
    }

    public Response queryRefresh(JsonArray urls, int pageNo, int pageSize, String startTime, String endTime) throws QiniuException {
        JsonObject req = new JsonObject();
        req.add("urls", urls);
        req.addProperty("pageNo", pageNo);
        req.addProperty("pageSize", pageSize);
        req.addProperty("startTime", startTime);
        req.addProperty("endTime", endTime);
        return UOperationForUrls(refreshQueryUrl, req);
    }

    public Response prefetch(String[] urls) throws QiniuException {
        Map<String, String[]> req = new HashMap<>();
        req.put("urls", urls);
        return UOperationForUrls(prefetchUrl, req);
    }

    public Response queryPrefetch(String[] urls) throws QiniuException {
        Map<String, String[]> req = new HashMap<>();
        req.put("urls", urls);
        return UOperationForUrls(prefetchQueryUrl, req);
    }

    public Response queryPrefetch(JsonArray urls, int pageNo, int pageSize, String startTime, String endTime) throws QiniuException {
        JsonObject req = new JsonObject();
        req.add("urls", urls);
        req.addProperty("pageNo", pageNo);
        req.addProperty("pageSize", pageSize);
        req.addProperty("startTime", startTime);
        req.addProperty("endTime", endTime);
        return UOperationForUrls(prefetchQueryUrl, req);
    }

    private Response UOperationForUrls(String apiUrl, Object req) throws QiniuException {
        byte[] body = Json.encode(req).getBytes(Constants.UTF_8);
        return client.post(apiUrl, body, auth.authorizationV2(apiUrl, "POST", body, Client.JsonMime), Client.JsonMime);
    }
}
