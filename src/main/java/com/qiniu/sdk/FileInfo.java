package com.qiniu.sdk;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.qiniu.util.Json;
import com.qiniu.util.JsonConvertUtils;

/**
 * list 接口的回复文件对象信息
 * 参考文档：<a href="https://developer.qiniu.com/kodo/api/list">资源列举</a>
 */
public class FileInfo {
    /**
     * 文件名
     */
    public String key;
    /**
     * 文件hash值
     */
    public String hash;
    /**
     * 文件大小，单位：字节
     */
    public long fsize;
    /**
     * 文件上传时间，单位为：100纳秒
     */
    public long putTime;
    /**
     * 文件的mimeType
     */
    public String mimeType;
    /**
     * 文件上传时设置的endUser
     */
    public String endUser;
    /**
     * 文件的存储类型，0为普通存储，1为低频存储
     */
    public int type;
    /**
     * 从当前位置的 file 得到的下一个 marker
     */
    public String nextMarker;
    /**
     * 标记是否是已删除的文件
     */
    public boolean isDelete;

    public String toString() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("key", key);
        jsonObject.addProperty("hash", hash);
        jsonObject.addProperty("fsize", fsize);
        jsonObject.addProperty("putTime", putTime);
        jsonObject.addProperty("mimeType", mimeType);
        jsonObject.addProperty("type", type);
        return JsonConvertUtils.toJsonWithoutUrlEscape(jsonObject);
    }
}