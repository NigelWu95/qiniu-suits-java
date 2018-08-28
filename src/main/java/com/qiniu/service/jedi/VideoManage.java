package com.qiniu.service.jedi;

import com.google.gson.Gson;
import com.qiniu.common.Const;
import com.qiniu.common.HttpClient;
import com.qiniu.common.QiniuAuth;
import com.qiniu.util.UrlSafeBase64;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by qiniu.
 * 视频管理
 * 包含 获取视频信息，获取视频列表，修改视频信息，删除视频信息等
 */
public class VideoManage {

    private static HttpClient httpClient;

    public VideoManage(QiniuAuth auth) {
        httpClient = HttpClient.getHttpClient(auth);
    }

    /**
     * GET /v1/hubs/<Hub>/videos/<EncodedVideoKey>?include_player=<IncludePlayer>
     */
    public String getVideoInfo(String hub, String videoKey) {
        String rawUrl = Const.JEDI_HOST + "/v1/hubs/" + hub + "/videos/" + UrlSafeBase64.encodeToString(videoKey);
        Map<String, Object> ret = httpClient.doRequest("GET", rawUrl, null, false, null);
        return new Gson().toJson(ret);
    }

    /*
     * GET /v1/hubs/<Hub>/videos?cursor=<Cursor>&count=<Count>
     */
    public Map<String, Object> getVideoInfoList(String hub, String cursor, Integer count) {
        if (count == null) {
            count = 100;
        }
        String rawUrl = Const.JEDI_HOST + "/v1/hubs/" + hub + "/videos?count=" + count;
        if (cursor != null && cursor.length() > 0) {
            rawUrl += "&cursor=" + cursor;
        }
        String auth = httpClient.getHttpRequestSign("GET", rawUrl, null, false);
        Map<String, Object> ret = httpClient.doRequest("GET", rawUrl, null, false, auth);
        return ret;
    }

    /*
     * PUT /v1/hubs/<Hub>/videos/<EncodedVideoKey>/thumbnails/active/<Active>
     */
    public boolean appointVideoThumbnails(String hub, String videoKey, int active) {
        String rawUrl = Const.JEDI_HOST + "/v1/hubs/" + hub + "/videos/" + UrlSafeBase64.encodeToString(videoKey)
                + "/thumbnails/active/" + active;

        String auth = httpClient.getHttpRequestSign("PUT", rawUrl, null, false);
        Map<String, Object> ret = httpClient.doRequest("PUT", rawUrl, null, false, auth);
        if ((Integer) ret.get("code") == 200) {
            return true;
        }
        return false;
    }

    /*
     * DELETE /v1/hubs/<Hub>/videos/<EncodedVideoKey>
     */
    public boolean deleteOneVideo(String hub, String videoKey) {
        String rawUrl = Const.JEDI_HOST + "/v1/hubs/" + hub + "/videos/" + UrlSafeBase64.encodeToString(videoKey);
        String auth = httpClient.getHttpRequestSign("DELETE", rawUrl, null, false);
        Map<String, Object> ret = httpClient.doRequest("DELETE", rawUrl, null, false, auth);
        if ((Integer) ret.get("code") == 200) {
            return true;
        }

        return false;
    }

    /*
     * DELETE /v1/hubs/<Hub>/videos
     */
    public String deleteBatchVideos(String hub, String[] videoKeys) {

        for (int i = 0; i < videoKeys.length; i++) {
            videoKeys[i] = UrlSafeBase64.encodeToString(videoKeys[i]);
        }
        Gson gson = new Gson();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("keys", videoKeys);
        String bodyStr = gson.toJson(map);

        String rawUrl = Const.JEDI_HOST + "/v1/hubs/" + hub + "/videos";
        String auth = httpClient.getHttpRequestSign("DELETE", rawUrl, bodyStr, true);
        Map<String, Object> ret = httpClient.doRequest("DELETE", rawUrl, bodyStr, true, auth);

        return gson.toJson(ret);
    }


}
