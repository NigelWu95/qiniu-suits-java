package com.qiniu.common;

import com.qiniu.util.Auth;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by qiniu.
 * 请求API的client，包括URL鉴权，发送请求等
 */
public class HttpClient {

    private Auth auth;

    private static volatile HttpClient httpClient = null;

    private HttpClient(Auth auth) {
        this.auth = auth;
    }

    /***
     * 加载到配置文件里的ak和sk
     * @return
     */
    public static HttpClient getHttpClient(Auth auth) {
        if (httpClient == null) {
            synchronized (HttpClient.class) {
                if (httpClient == null) {
                    httpClient = new HttpClient(auth);
                }
            }
        }

        return httpClient;
    }


    /**
        传参数类型：
            http  method
            完整的请求URL
            请求body，如果没有的话填null
            是否有content-type ，没有的话填null
            是否需要鉴权，没有的话填null

        return：API的返回值，以键值对的形式返回，code存放http状态码，msg存放response body
     **/
    public Map<String, Object> doRequest(String method, String rawUrl, String bodyStr, boolean hasContype, String auth) {
        Map<String, Object> resMap = new HashMap<String, Object>();
        int status = -1;

        try {
            URL url = new URL(rawUrl);
            HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
            httpCon.setRequestMethod(method.toUpperCase());

            if (hasContype) {
                httpCon.setRequestProperty("Content-Type", "application/json");
            }

            if (auth != null && auth.length() > 0) {
                httpCon.setRequestProperty("Authorization", auth);
            }

            httpCon.setDoOutput(true);
            httpCon.setDoInput(true);


            OutputStreamWriter out = null;
            if (bodyStr != null && bodyStr.length() > 0) {
                out = new OutputStreamWriter(
                        httpCon.getOutputStream());
                out.write(bodyStr);
                out.flush();
                out.close();
            }

            status = httpCon.getResponseCode();  //get http status

            BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream()));
            String temp = null;
            StringBuilder sb = new StringBuilder();
            while ((temp = in.readLine()) != null) {
                sb.append(temp);
            }
            String result = sb.toString();
            in.close();


            if (result != null && result.length() > 0) {
                result = result.trim();
            }

            resMap.put("code", status);
            resMap.put("msg", result);

        } catch (Exception e) {
            resMap.put("code", status);
            resMap.put("msg", e.getMessage());
        }
        return resMap;
    }

    /***
     传参数类型：
         http  method
         完整的请求URL
         请求body，如果没有的话填null
         是否有content-type ，没有的话填null
         是否需要鉴权，没有的话填null
     return: 返回鉴权字符串
     */
    public String getHttpRequestSign(String method, String rawUrl, String bodyStr, boolean hasContype) {

        method=method.toUpperCase();

        String contentType = null;
        if (hasContype) {
            contentType = "application/json";
        }

        String host = null;
        String uri = null;
        String rawQuery = null;

        URL connURL = null;
        try {
            connURL = new URL(rawUrl);
        } catch (MalformedURLException e) {
            System.out.println("URL MalformedURLException encountered:" + e);
        }

        host = connURL.getHost();
        uri = connURL.getPath();
        rawQuery = connURL.getQuery();

        String data = method + " " + uri;

        if (rawQuery != null && rawQuery.length() > 0) {
            data += "?" + rawQuery;
        }

        data += "\nHost: " + host;

        if (contentType != null && contentType.length() > 0) {
            data += "\nContent-Type: " + contentType;
        }

        data += "\n\n";

        if (contentType != null && contentType.length() > 0 && bodyStr != null && bodyStr.length() > 0 && (!contentType.equals("application/octet-stream"))) {
            data += bodyStr;
        }

        try {
            return "Qiniu " + auth.sign(data);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
