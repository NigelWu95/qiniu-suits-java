package com.qiniu.util;

import com.aliyun.oss.model.OSSObjectSummary;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.qcloud.cos.model.COSObjectSummary;
import com.qiniu.common.Constants;
import com.qiniu.storage.model.FileInfo;

import java.util.HashMap;
import java.util.Map;

public class OssUtils {

    public static Map<String, Integer> aliStatus = new HashMap<String, Integer>(){{
        put("UnknownHost", 400); // 错误的 region 等
        put("AccessDenied", 403);// 拒绝访问
        put("BucketAlreadyExists", 409);// 存储空间已经存在
        put("BucketNotEmpty", 409);// 存储空间非空
        put("EntityTooLarge", 400);// 实体过大
        put("EntityTooSmall", 400);// 实体过小
        put("FileGroupTooLarge", 400);// 文件组过大
        put("FilePartNotExist", 400);// 文件分片不存在
        put("FilePartStale", 400);// 文件分片过时
        put("InvalidArgument", 400);// 参数格式错误
        put("InvalidAccessKeyId", 403);// AccessKeyId不存在
        put("InvalidBucketName", 400);// 无效的存储空间名称
        put("InvalidDigest", 400);// 无效的摘要
        put("InvalidObjectName", 400);// 无效的文件名称
        put("InvalidPart", 400);// 无效的分片
        put("InvalidPartOrder", 400);// 无效的分片顺序
        put("InvalidTargetBucketForLogging", 400);// Logging操作中有无效的目标存储空间
        put("InternalError", 500);// OSS内部错误
        put("MalformedXML", 400);// XML格式非法
        put("MethodNotAllowed", 405);// 不支持的方法
        put("MissingArgument", 411);// 缺少参数
        put("MissingContentLength", 411);// 缺少内容长度
        put("NoSuchBucket", 404);// 存储空间不存在
        put("NoSuchKey", 404);// 文件不存在
        put("NoSuchUpload", 404);// 分片上传ID不存在
        put("NotImplemented", 501);// 无法处理的方法
        put("PreconditionFailed", 412);// 预处理错误
        put("RequestTimeTooSkewed", 403);// 客户端本地时间和OSS服务器时间相差超过15分钟
        put("RequestTimeout", 400);// 请求超时
        put("SignatureDoesNotMatch", 403);// 签名错误
        put("InvalidEncryptionAlgorithmError", 400);// 指定的熵编码加密算法错误
    }};

    public static int AliStatusCode(String error, int Default) {
        return aliStatus.getOrDefault(error, Default);
    }

    public static String getQiniuMarker(FileInfo fileInfo) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("k", fileInfo.key);
        return Base64.encodeToString(JsonUtils.toJson(jsonObject).getBytes(Constants.UTF_8),
                Base64.URL_SAFE | Base64.NO_WRAP);
    }

    public static String getAliOssMarker(OSSObjectSummary ossObjectSummary) {
        return ossObjectSummary.getKey();
    }

    public static String getTenCosMarker(COSObjectSummary cosObjectSummary) {
        return cosObjectSummary.getKey();
    }

    public static String getQiniuMarker(String key) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("k", key);
        return Base64.encodeToString(JsonUtils.toJson(jsonObject).getBytes(Constants.UTF_8),
                Base64.URL_SAFE | Base64.NO_WRAP);
    }

    public static String getAliOssMarker(String key) {
        return key;
    }

    public static String getTenCosMarker(String key) {
        return key;
    }

    public static String decodeQiniuMarker(String marker) {
        String decodedMarker = new String(Base64.decode(marker, Base64.URL_SAFE | Base64.NO_WRAP));
        JsonObject jsonObject = new JsonParser().parse(decodedMarker).getAsJsonObject();
        return jsonObject.get("k").getAsString();
    }

    public static String decodeAliOssMarker(String marker) {
        return marker;
    }

    public static String decodeTenCosMarker(String marker) {
        return marker;
    }
}
