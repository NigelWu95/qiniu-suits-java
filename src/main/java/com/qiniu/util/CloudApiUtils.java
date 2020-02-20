package com.qiniu.util;

import com.aliyun.oss.*;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.OSSObjectSummary;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ObsObject;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.Bucket;
import com.qcloud.cos.model.COSObjectSummary;
import com.qiniu.common.Constants;
import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IFileChecker;
import com.qiniu.sdk.FileItem;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.sdk.UpYunConfig;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.Region;
import com.qiniu.storage.model.BucketInfo;
import com.qiniu.storage.model.FileInfo;

import java.io.IOException;
import java.util.*;
import java.util.Base64.*;

public final class CloudApiUtils {

    public static String QINIU = "qiniu";
    public static String TENCENT = "tencent";
    public static String ALIYUN = "aliyun";
    public static String AWSS3 = "s3";
    public static String UPYUN = "upyun";
    public static String NETYUN = "netease";
    public static String HUAWEI = "huawei";
    public static String BAIDU = "baidu";
    public static String LOCAL = "local";
    public static String TYPE_Storage = "storage";
    public static String TYPE_File = "file";

    public static Map<String, String> datasourceMap = new HashMap<String, String>(){{
        put(QINIU, TYPE_Storage);
        put(TENCENT, TYPE_Storage);
        put(ALIYUN, TYPE_Storage);
        put(AWSS3, TYPE_Storage);
        put(UPYUN, TYPE_Storage);
        put(NETYUN, TYPE_Storage);
        put(HUAWEI, TYPE_Storage);
        put(BAIDU, TYPE_Storage);
        put(LOCAL, TYPE_File);
    }};

    public static final String QINIU_RS_BATCH_URL = "http://rs.qiniu.com/batch";

    public static String getSourceType(String source) {
        return datasourceMap.get(source);
    }

    public static boolean isStorageSource(String source) {
        return TYPE_Storage.equals(getSourceType(source));
    }

    public static boolean isFileSource(String source) {
        return TYPE_File.equals(getSourceType(source));
    }

    public static Encoder encoder = java.util.Base64.getEncoder();
    public static Decoder decoder = java.util.Base64.getDecoder();

    public static Map<String, Integer> aliStatus = new HashMap<String, Integer>(){{
        put("InvalidResponse", 400); // 测试过程中发现的异常响应
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

    public static int NetStatusCode(String error, int Default) {
        return aliStatus.getOrDefault(error, Default);
    }

    public static String getQiniuMarker(FileInfo fileInfo) {
        return getQiniuMarker(fileInfo.key);
    }

    public static String getAliOssMarker(OSSObjectSummary summary) {
        return summary.getKey();
    }

    public static String getTenCosMarker(COSObjectSummary summary) {
        return summary.getKey();
    }

    public static String getHuaweiObsCosMarker(ObsObject obsObject) {
        return obsObject.getObjectKey();
    }

    public static String getbaiduBosCosMarker(BosObjectSummary obsObject) {
        return obsObject.getKey();
    }

    public static String getUpYunMarker(String bucket, FileItem fileItem) {
        if (fileItem.key.contains("/")) {
            String convertedKey = fileItem.key.replace("/", "/~");
            int lastIndex = convertedKey.lastIndexOf("~");
            if ("folder".equals(fileItem.attribute) || ("F".equals(fileItem.attribute))) {
                convertedKey = convertedKey.substring(0, lastIndex) + "@" + convertedKey.substring(lastIndex);
            } else {
                convertedKey = convertedKey.substring(0, lastIndex) + "@#" + convertedKey.substring(lastIndex + 1);
            }
            return new String(encoder.encode((bucket + "/~" + convertedKey).getBytes()));
        } else {
            return ("folder".equals(fileItem.attribute) || ("F".equals(fileItem.attribute))) ?
                bucket + "/@~" + fileItem.key : bucket + "/@#" + fileItem.key;
        }
    }

    public static String getQiniuMarker(String key) {
        if (key == null || "".equals(key)) return null;
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("c", 0);
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

    public static String getUpYunMarker(String username, String password, String bucket, String key) throws SuitsException {
        if (key == null || "".equals(key)) return null;
        UpYunClient upYunClient = new UpYunClient(new UpYunConfig(), username, password);
        try {
            return getUpYunMarker(bucket, upYunClient.getFileInfo(bucket, key));
        } catch (Exception e) {
            throw new SuitsException(e, 848);
        } finally {
            upYunClient = null;
        }
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

    public static String decodeHuaweiObsMarker(String marker) {
        return marker;
    }

    public static String decodeUpYunMarker(String marker) {
        String keyString = new String(decoder.decode(marker));
        int index = keyString.contains("/~") ? keyString.indexOf("/~") + 2 : keyString.indexOf("/") + 1;
        return keyString.substring(index).replace("(/~|/@~|/@#)", "/");
    }

    public static void checkQiniu(Auth auth) throws IOException {
        try {
            BucketManager bucketManager = new BucketManager(auth, new Configuration());
            bucketManager.buckets();
            bucketManager = null;
        } catch (QiniuException e) {
            if (e.response != null) e.response.close();
            throw e;
        }
    }

    public static void checkQiniu(BucketManager bucketManager, String bucket) throws IOException {
        try {
            Response response = bucketManager.listV2(bucket, null, null, 1, null);
            response.close();
            response = null;
        } catch (QiniuException e) {
            if (e.response != null) e.response.close();
            throw e;
        }
    }

    public static void checkQiniu(String accessKey, String secretKey, Configuration configuration, String bucket)
            throws IOException {
        try {
            BucketManager bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration);
            Response response = bucketManager.listV2(bucket, null, null, 1, null);
            response.close();
            response = null;
            bucketManager = null;
        } catch (QiniuException e) {
            if (e.response != null) e.response.close();
            throw e;
        }
    }

    public static void checkAliyun(OSSClient ossClient) {
        ossClient.listBuckets();
    }

    public static void checkTencent(COSClient cosClient) {
        cosClient.listBuckets();
    }

    public static void checkAws(AmazonS3 s3Client) {
        s3Client.listBuckets();
    }

    public static void checkHuaWei(ObsClient obsClient) {
        obsClient.listBucketsV2(null);
    }

    public static void checkBaidu(BosClient bosClient) {
        bosClient.listBuckets();
    }

    public static Region getQiniuRegion(String regionName) throws IOException {
        if (regionName == null || "".equals(regionName)) return Region.autoRegion();
        switch (regionName) {
            case "z0":
            case "huadong":
                Region.Builder builder = new Region.Builder(Region.huadong());
                return builder.iovipHost("iovip-z0.qbox.me")
                        .rsfHost("rsf-z0.qbox.me")
                        .rsHost("rs-z0.qbox.me")
//                        .apiHost("api-z0.qiniu.com")
                        .build();
            case "z1":
            case "huabei": return Region.huabei();
            case "z2":
            case "huanan": return Region.huanan();
            case "na0":
            case "beimei": return Region.beimei();
            case "as0":
            case "xinjiapo": return Region.xinjiapo();
            case "qvm-z0":
            case "qvm-huadong": return Region.qvmHuadong();
            case "qvm-z1":
            case "qvm-huabei": return Region.qvmHuabei();
            default: throw new IOException("no such region");
        }
    }

    public static String getQiniuRegion(String accessKey, String secretKey, String bucket) throws SuitsException {
        try {
            Auth auth = Auth.create(accessKey, secretKey);
            Configuration configuration = new Configuration();
            BucketManager bucketManager = new BucketManager(auth, configuration);
            BucketInfo bucketInfo = bucketManager.getBucketInfo(bucket);
            bucketManager = null;
            configuration = null;
            auth = null;
            return bucketInfo.getRegion();
        } catch (QiniuException e) {
            if (e.response != null) e.response.close();
            throw new SuitsException(e, e.code());
        }
    }

    public static String getAliOssRegion(String accessKeyId, String accessKeySecret, String bucket) throws SuitsException {
        CredentialsProvider credentialsProvider = new DefaultCredentialProvider(accessKeyId, accessKeySecret);
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        OSSClient ossClient = new OSSClient("oss-cn-shanghai.aliyuncs.com", credentialsProvider, clientConfiguration);
        try {
            return ossClient.getBucketLocation(bucket);
        } catch (ClientException e) {
            throw new SuitsException(e, CloudApiUtils.AliStatusCode(e.getErrorCode(), -1), "get aliyun region failed");
        } catch (ServiceException e) {
            throw new SuitsException(e, CloudApiUtils.AliStatusCode(e.getErrorCode(), -1), "get aliyun region failed");
        } finally {
            ossClient.shutdown();
            ossClient = null;
            clientConfiguration = null;
            credentialsProvider = null;
        }
//        OSSClient ossClient = new OSSClient("oss.aliyuncs.com", credentialsProvider, clientConfiguration);
//        // 阿里 oss sdk listBuckets 能迭代列举出所有空间
//        List<com.aliyun.oss.model.Bucket> list = ossClient.listBuckets();
//        for (com.aliyun.oss.model.Bucket eachBucket : list) {
//            if (eachBucket.getName().equals(bucket)) return eachBucket.getLocation();
//        }
//        throw new SuitsException(-1, "can not find this bucket.");
    }

    public static String getTenCosRegion(String secretId, String secretKey, String bucket) throws SuitsException {
        COSCredentials cred = new BasicCOSCredentials(secretId, secretKey);
        ClientConfig clientConfig = new ClientConfig();
        COSClient cosClient = new COSClient(cred, clientConfig);
        // 腾讯 cos sdk listBuckets 不进行分页列举，账号空间个数上限为 200，可一次性列举完
        try {
            List<Bucket> list = cosClient.listBuckets();
            String region = null;
            for (Bucket eachBucket : list) {
                if (eachBucket.getName().equals(bucket)) {
                    region = eachBucket.getLocation();
                    break;
                }
            }
            if (region != null) return region;
        } catch (CosServiceException e) {
            throw new SuitsException(e, e.getStatusCode(), "get tencent region failed");
        } catch (CosClientException e) {
            throw new SuitsException(e, -1, "get tencent region failed");
        } finally {
            cosClient.shutdown();
            cosClient = null;
            clientConfig = null;
            cred = null;
        }
        throw new SuitsException(400, "can not find this bucket.");
    }

    public static String getS3Region(String s3AccessKeyId, String s3SecretKey, String bucket) throws SuitsException {
        AWSCredentials credentials = new BasicAWSCredentials(s3AccessKeyId, s3SecretKey);
        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.DEFAULT_REGION)
                .build();
        try {
            return s3Client.getBucketLocation(bucket);
        } catch (AmazonServiceException e) {
            throw new SuitsException(e, e.getStatusCode(), "get s3 region failed");
        } catch (SdkClientException e) {
            throw new SuitsException(e, -1, "get s3 region failed");
        } finally {
            s3Client.shutdown();
            s3Client = null;
            credentialsProvider = null;
            credentials = null;
        }
    }

    public static String getHuaweiObsRegion(String accessKeyId, String secretKey, String bucket) throws SuitsException {
        ObsClient obsClient = new ObsClient(accessKeyId, secretKey, "https://obs.myhuaweicloud.com");
        try {
            return obsClient.getBucketLocation(bucket);
        } catch (ObsException e) {
            throw new SuitsException(e, e.getResponseCode(), "get huaweicloud region failed");
        } finally {
            obsClient = null;
        }
    }

    public static String getBaiduBosRegion(String accessKeyId, String secretKey, String bucket) {
        BosClient bosClient = new BosClient(new BosClientConfiguration()
//                .withEndpoint("bj.bcebos.com")
                .withCredentials(new DefaultBceCredentials(accessKeyId, secretKey)));
        try {
            return bosClient.getBucketLocation(bucket).getLocationConstraint();
        } finally {
            bosClient.shutdown();
        }
    }

    private static final String lineSeparator = System.getProperty("line.separator");

    public static String upYunSign(String method, String date, String path, String userName, String password, String md5)
            throws IOException {
        StringBuilder sb = new StringBuilder();
        String sp = "&";
        sb.append(method);
        sb.append(sp);
        sb.append(path);
        sb.append(sp);
        sb.append(date);

        if (md5 != null && md5.length() > 0) {
            sb.append(sp);
            sb.append(md5);
        }
        String raw = sb.toString().trim();
        byte[] hmac;
        try {
            hmac = EncryptUtils.calculateRFC2104HMACRaw(password, raw);
        } catch (Exception e) {
            throw new IOException("calculate SHA1 wrong.");
        }

        if (hmac != null) {
            return String.join("", "UPYUN ", userName, ":",
                    EncryptUtils.encodeLines(hmac, 0, hmac.length, 76, lineSeparator).trim());
        }

        return null;
    }

    public static <T> List<T> initFutureList(int limit, int times) {
        int futureSize = limit;
        if (limit < 1000) {
            futureSize += limit * times;
        } else if (limit <= 5000) {
            futureSize += 10000;
        } else {
            futureSize += limit;
        }
        return new ArrayList<>(futureSize);
    }

    public static IFileChecker fileCheckerInstance(BucketManager bucketManager, String bucket) {
        return key -> {
            Response response;
            try {
                response = bucketManager.statResponse(bucket, key);
            } catch (QiniuException e) {
                if (e.response != null) e.response.close();
                return null;
            }
            if (response.statusCode == 200) {
                try {
                    return response.bodyString();
                } finally {
                    response.close();
                }
            }
            return null;
        };
    }
}
