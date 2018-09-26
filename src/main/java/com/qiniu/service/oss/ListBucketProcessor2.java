package com.qiniu.service.oss;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.qiniu.common.*;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.JSONConvertUtils;
import com.qiniu.util.StringMap;
import com.qiniu.util.StringUtils;
import com.qiniu.util.UrlSafeBase64;

import java.io.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class ListBucketProcessor2 {

    private QiniuAuth auth;
    private Configuration configuration;
    private QiniuBucketManager bucketManager;
    private Client client;
    private FileReaderAndWriterMap targetFileReaderAndWriterMap;

    private static volatile ListBucketProcessor2 listBucketProcessor = null;

    public ListBucketProcessor2(QiniuAuth auth, Configuration configuration, FileReaderAndWriterMap targetFileReaderAndWriterMap) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucketManager = new QiniuBucketManager(auth, configuration);
        this.client = new Client();
        this.targetFileReaderAndWriterMap = targetFileReaderAndWriterMap;
    }

    public static ListBucketProcessor2 getChangeStatusProcessor(QiniuAuth auth, Configuration configuration, FileReaderAndWriterMap targetFileReaderAndWriterMap) {
        if (listBucketProcessor == null) {
            synchronized (ListBucketProcessor2.class) {
                if (listBucketProcessor == null) {
                    listBucketProcessor = new ListBucketProcessor2(auth, configuration, targetFileReaderAndWriterMap);
                }
            }
        }
        return listBucketProcessor;
    }

    /*
    v2 的 list 接口，接收到响应后通过 java8 的流来处理响应的文本流。
     */
    public String doFileListV2(String bucket, String prefix, String delimiter, String marker, int limit, String endFile,
                               IOssFileProcess iOssFileProcessor, boolean withParallel, int retryCount) {

        Response response = null;
        InputStream inputStream;
        Reader reader;
        BufferedReader bufferedReader;
        AtomicBoolean endFlag = new AtomicBoolean(false);
        AtomicReference<String> endMarker = new AtomicReference<>();

        try {
            response = listV2(bucket, prefix, delimiter, marker, limit, retryCount);
            inputStream = new BufferedInputStream(response.bodyStream());
            reader = new InputStreamReader(inputStream);
            bufferedReader = new BufferedReader(reader);
            Stream<String> lineStream = withParallel ? bufferedReader.lines().parallel() : bufferedReader.lines();
            lineStream.forEach(line -> {
                try {
                    String fileInfoStr = getFileInfoV2AndMarker(bucket, line, retryCount)[0];
                    if (endFile.equals(JSONConvertUtils.toJson(fileInfoStr).get("key").getAsString())) {
                        endFlag.set(true);
                        endMarker.set(null);
                    }
                    if (!endFlag.get()) {
                        targetFileReaderAndWriterMap.writeSuccess(fileInfoStr);
                        iOssFileProcessor.processFile(fileInfoStr);
                        endMarker.set(JSONConvertUtils.toJson(line).get("marker").getAsString());
                    }
                } catch (QiniuSuitsException e) {
                    targetFileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefix + "\t" + delimiter + "\t" + marker
                            + "\t" + limit + "\t" + line + "\t" + e.getMessage());
                }
            });
            inputStream.close();
            reader.close();
            bufferedReader.close();
        } catch (IOException e) {
            targetFileReaderAndWriterMap.writeOther(bucket + "\t" + prefix + "\t" + delimiter + "\t" + marker
                    + "\t" + limit + "\t" + "{\"msg\":\"" + e.getMessage() + "\"}");
        } catch (QiniuSuitsException e) {
            targetFileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefix + "\t" + delimiter + "\t" + marker
                    + "\t" + limit + "\t" + e.getMessage());
        } finally {
            if (response != null) {
                response.close();
            }
        }

        return endMarker.get();
    }

    /*
    v2 的 list 接口，通过文本流的方式返回文件信息。
     */
    public Response listV2(String bucket, String prefix, String delimiter, String marker, int limit, int retryCount) throws QiniuSuitsException {

        prefix = prefix.replaceAll("\\s", "%20").replaceAll("\\\\", "%5C").replaceAll("%", "%25");
        String prefixParam = StringUtils.isNullOrEmpty(prefix) ? "" : "&prefix=" + prefix;
        String delimiterParam = StringUtils.isNullOrEmpty(delimiter) ? "" : "&delimiter=" + delimiter;
        String limitParam = limit == 0 ? "" : "&limit=" + limit;
        String markerParam = StringUtils.isNullOrEmpty(marker) ? "" : "&marker=" + marker;
        String url = "http://rsf.qbox.me/v2/list?bucket=" + bucket + prefixParam + delimiterParam + limitParam + markerParam;
        String authorization = "QBox " + auth.signRequest(url, null, null);
        StringMap headers = new StringMap().put("Authorization", authorization);

        return httpPostWithRetry(url, null, headers, null, retryCount);
    }

    private Response httpPostWithRetry(String url, byte[] body, StringMap headers, String contentType, int retryCount) throws QiniuSuitsException {
        Response response;

        try {
            response = client.post(url, null, headers, null);
        } catch (QiniuException e) {
            if (retryCount > 0 && String.valueOf(e.code()).matches("^[-015]\\d{0,2}")) {
                System.out.println(e.getMessage() + ", last " + retryCount + " times retry...");
                retryCount--;
                response = httpPostWithRetry(url, body, headers, contentType, retryCount);
            } else {
                throw new QiniuSuitsException(e);
            }
        }

        return response;
    }

    public String[] getFileInfoV2AndMarker(String bucket, String prefix, String delimiter, String marker, int limit, int retryCount)
            throws QiniuSuitsException {
        Response response = null;
        String[] fileInfoAndMarker;
        try {
            response = listV2(bucket, prefix, delimiter, marker, limit, retryCount);
            fileInfoAndMarker = getFileInfoV2AndMarker(bucket, response.bodyString(), retryCount);
        } catch (QiniuSuitsException e) {
            throw e;
        } catch (IOException e) {
            throw new QiniuSuitsException(e);
        } finally {
            if (response != null)
                response.close();
        }

        return fileInfoAndMarker;
    }

    public String[] getFileInfoV2AndMarker(String bucket, String line, int retryCount) throws QiniuSuitsException {
        String fileKey;
        String fileInfoStr;
        String retMarker;

        if (StringUtils.isNullOrEmpty(line))
            throw new QiniuSuitsException("line is empty");
        JsonObject json = JSONConvertUtils.toJson(line);
        JsonElement jsonElement = json.get("item");
        if (jsonElement == null || "null".equals(jsonElement.toString())) {
            if (json.get("marker") != null && !json.get("marker").getAsString().equals("")) {
                retMarker = json.get("marker").getAsString();
                JsonObject decodedMarker = JSONConvertUtils.toJson(new String(UrlSafeBase64.decode(retMarker)));
                fileKey = decodedMarker.get("k").getAsString();
                fileInfoStr = statWithRetry(bucket, fileKey, retryCount);
            } else
                throw new QiniuSuitsException("marker is empty");
        } else {
            fileInfoStr = JSONConvertUtils.toJson(json.getAsJsonObject("item"));
            retMarker = json.get("marker").getAsString();
        }

        return new String[]{fileInfoStr, retMarker};
    }

    private String statWithRetry(String bucket, String fileKey, int retryCount) throws QiniuSuitsException {
        String fileInfoStr;

        try {
            fileInfoStr = JSONConvertUtils.toJson(bucketManager.stat(bucket, fileKey));
        } catch (QiniuException e) {
            retryCount--;
            if (retryCount > 0 && String.valueOf(e.code()).matches("^[-015]\\d{0,2}")) {
                System.out.println(e.getMessage() + ", last " + retryCount + " times retry...");
                retryCount--;
                fileInfoStr = statWithRetry(bucket, fileKey, retryCount);
            } else {
                throw new QiniuSuitsException(e);
            }
        }

        return fileInfoStr;
    }

    public void closeBucketManager() {
        if (bucketManager != null)
            bucketManager.closeResponse();
    }
}