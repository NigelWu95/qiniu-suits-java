package com.qiniu.process.qiniu;

import com.qiniu.common.Constants;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.Json;

import java.util.HashMap;

public class CdnHelper {

    private final Auth auth;
    private final Client client;
    private static final String refreshUrl = "http://fusion.qiniuapi.com/v2/tune/refresh";
    private static final String prefetchUrl = "http://fusion.qiniuapi.com/v2/tune/prefetch";

    public CdnHelper(Auth auth) {
        this.auth = auth;
        this.client = new Client();
    }

    public CdnHelper(Auth auth, Configuration configuration) {
        this.auth = auth;
        this.client = new Client(configuration);
    }

    public Response refresh(String[] urls, String[] dirs) throws QiniuException {
        HashMap<String, String[]> req = new HashMap<>();
        if (urls != null) {
            req.put("urls", urls);
        }
        if (dirs != null) {
            req.put("dirs", dirs);
        }
        byte[] body = Json.encode(req).getBytes(Constants.UTF_8);
        return client.post(refreshUrl, body, auth.authorizationV2(refreshUrl, "POST", body, Client.JsonMime), Client.JsonMime);
    }

    public Response prefetch(String[] urls) throws QiniuException {
        HashMap<String, String[]> req = new HashMap<>();
        req.put("urls", urls);
        byte[] body = Json.encode(req).getBytes(Constants.UTF_8);
        return client.post(prefetchUrl, body, auth.authorizationV2(prefetchUrl, "POST", body, Client.JsonMime), Client.JsonMime);
    }
}
