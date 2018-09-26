package com.qiniu.service.oss;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.qiniu.common.*;
import com.qiniu.common.QiniuBucketManager.*;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.*;

import java.io.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class ListBucketProcessor {

    private QiniuAuth auth;
    private Configuration configuration;
    private QiniuBucketManager bucketManager;
    private Client client;
    private FileReaderAndWriterMap targetFileReaderAndWriterMap;

    private static volatile ListBucketProcessor listBucketProcessor = null;

    public ListBucketProcessor(QiniuAuth auth, Configuration configuration, FileReaderAndWriterMap targetFileReaderAndWriterMap) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucketManager = new QiniuBucketManager(auth, configuration);
        this.client = new Client();
        this.targetFileReaderAndWriterMap = targetFileReaderAndWriterMap;
    }

    public static ListBucketProcessor getChangeStatusProcessor(QiniuAuth auth, Configuration configuration, FileReaderAndWriterMap targetFileReaderAndWriterMap) {
        if (listBucketProcessor == null) {
            synchronized (ListBucketProcessor.class) {
                if (listBucketProcessor == null) {
                    listBucketProcessor = new ListBucketProcessor(auth, configuration, targetFileReaderAndWriterMap);
                }
            }
        }
        return listBucketProcessor;
    }

    /*
    单次列举，可以传递 marker 和 limit 参数，通常采用此方法进行并发处理
     */
    public String doFileList(String bucket, String prefix, String delimiter, String marker, int limit, String endFile,
                             IOssFileProcess iOssFileProcessor, int retryCount) {

        String endMarker = null;

        try {
            FileListing fileListing = listFilesWithRetry(bucket, prefix, delimiter, marker, limit, retryCount);
            checkFileListing(fileListing);
            FileInfo[] items = fileListing.items;
            String fileInfoStr;

            for (int i = 0; i < items.length; i++) {
                if (items[i].key.equals(endFile)) {
                    fileListing = null;
                    break;
                }
                fileInfoStr = JSONConvertUtils.toJson(items[i]);
                targetFileReaderAndWriterMap.writeSuccess(fileInfoStr);
                if (iOssFileProcessor != null) {
                    iOssFileProcessor.processFile(fileInfoStr, retryCount);
                }
            }

            endMarker = fileListing == null ? null : fileListing.marker;
        } catch (QiniuSuitsException e) {
            targetFileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefix + "\t" + delimiter + "\t" + marker
                    + "\t" + limit + "\t" + e.getMessage());
        }

        return endMarker;
    }


    public FileListing listFilesWithRetry(String bucket, String prefix, String delimiter, String marker, int limit, int retryCount)
            throws QiniuSuitsException {
        StringMap map = new StringMap().put("bucket", bucket).putNotEmpty("marker", marker)
                .putNotEmpty("prefix", prefix).putNotEmpty("delimiter", delimiter).putWhen("limit", limit, limit > 0);

        String url = String.format("%s/list?%s", configuration.rsfHost(auth.accessKey, bucket), map.formString());
        StringMap headers = auth.authorization(url);
        Response response = null;
        String responseBody = "";
        FileListing fileListing = null;

        try {
            response = client.get(url, headers);
            responseBody = response.bodyString();
        } catch (QiniuException e1) {
            if (retryCount <= 0)
                throw new QiniuSuitsException(e1);
            while (retryCount > 0) {
                try {
                    System.out.println(e1.getMessage() + ", last " + retryCount + " times retry...");
                    response = client.get(url, headers);
                    responseBody = response.bodyString();
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        } finally {
            if (response != null)
                response.close();
        }

        try {
            fileListing = JSONConvertUtils.fromJson(responseBody, FileListing.class);
        } catch (JsonSyntaxException e3) {
            throw new QiniuSuitsException(e3);
        }

        return fileListing;
    }

    /*
    迭代器方式列举带 prefix 前缀的所有文件，直到列举完毕，limit 为单次列举的文件个数
     */
    public void doFileIteratorList(String bucket, String prefix, String endFile, int limit, IOssFileProcess iOssFileProcessor) {

        FileListIterator fileListIterator = bucketManager.createFileListIterator(bucket, prefix, limit, null);

        loop:while (fileListIterator.hasNext()) {
            FileInfo[] items = fileListIterator.next();

            for (FileInfo fileInfo : items) {
                if (fileInfo.key.equals(endFile)) {
                    break loop;
                }
                targetFileReaderAndWriterMap.writeSuccess(JSONConvertUtils.toJson(fileInfo));
                if (iOssFileProcessor != null) {
                    iOssFileProcessor.processFile(JSONConvertUtils.toJson(fileInfo), 3);
                }
            }
        }
    }

    public void checkFileListing(FileListing fileListing) throws QiniuSuitsException {
        if (fileListing == null)
            throw new QiniuSuitsException("line is empty");
    }

    public String[] getFirstFileInfoAndMarker(String bucket, String prefix, String delimiter, String marker, int limit,
                                              int index, int retryCount) throws QiniuSuitsException {
        FileListing fileListing = listFilesWithRetry(bucket, prefix, delimiter, marker, limit, retryCount);
        return getFirstFileInfoAndMarker(bucket, fileListing, index, retryCount);
    }

    public String[] getFirstFileInfoAndMarker(String bucket, FileListing fileListing, int index, int retryCount) throws QiniuSuitsException {
        checkFileListing(fileListing);
        FileInfo[] items = fileListing.items;
        String marker = fileListing.marker;
        String fileInfoStr = items.length > 0 ? JSONConvertUtils.toJson(items[index]) : getFileInfoByMarker(bucket, marker, retryCount);
        String retMarker = items.length < 1000 ? null : marker;

        return new String[]{fileInfoStr, retMarker};
    }

    public String getFileInfoByMarker(String bucket, String marker, int retryCount)throws QiniuSuitsException {
        String fileKey;
        String fileInfoStr;

        if (!StringUtils.isNullOrEmpty(marker)) {
            JsonObject decodedMarker = JSONConvertUtils.toJson(new String(UrlSafeBase64.decode(marker)));
            fileKey = decodedMarker.get("k").getAsString();
            fileInfoStr = statWithRetry(bucket, fileKey, retryCount);
        } else
            throw new QiniuSuitsException("marker is empty");

        return fileInfoStr;
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
                    String fileInfoStr = getFileInfoV2AndMarker(line)[0];
                    if (endFile.equals(JSONConvertUtils.toJson(fileInfoStr).get("key").getAsString())) {
                        endFlag.set(true);
                        endMarker.set(null);
                    }
                    if (!endFlag.get()) {
                        targetFileReaderAndWriterMap.writeSuccess(fileInfoStr);
                        iOssFileProcessor.processFile(fileInfoStr, retryCount);
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

    public String[] getFileInfoV2AndMarker(String bucket, String prefix, String delimiter, String marker, int limit, int retryCount)
            throws QiniuSuitsException {
        Response response = null;
        String[] fileInfoAndMarker;
        try {
            response = listV2(bucket, prefix, delimiter, marker, limit, retryCount);
            fileInfoAndMarker = getFileInfoV2AndMarker(response.bodyString());
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

    public String[] getFileInfoV2AndMarker(String line) throws QiniuSuitsException {
        JsonObject json;
        JsonElement item;
        String retMarker;
        String dir;
        String fileInfoStr;

        if (StringUtils.isNullOrEmpty(line))
            throw new QiniuSuitsException("line is empty");

        try {
            json = JSONConvertUtils.toJson(line);
            item = json.get("item");
            retMarker = json.get("marker").getAsString();
            dir = json.get("dir").getAsString();
        } catch (JsonSyntaxException e) {
            throw new QiniuSuitsException("line is not json");
        }

        if (item == null || StringUtils.isNullOrEmpty(item.getAsString())) {
            if (StringUtils.isNullOrEmpty(dir)) {
                QiniuSuitsException suitsException = new QiniuSuitsException("this item is deleted");
                suitsException.addToFieldMap("marker", retMarker);
                throw suitsException;
            } else {
                if (StringUtils.isNullOrEmpty(retMarker)) {
                    throw new QiniuSuitsException("marker is empty");
                } else {
                    fileInfoStr = "{\"dir\":\"" + dir + "\"}";
                }
            }
        } else {
            fileInfoStr = JSONConvertUtils.toJson(json.getAsJsonObject("item"));
            retMarker = json.get("marker").getAsString();
        }

        return new String[]{fileInfoStr, retMarker};
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
        Response response = null;

        try {
            response = client.post(url, body, headers, contentType);
        } catch (QiniuException e1) {
            if (retryCount <= 0)
                throw new QiniuSuitsException(e1);
            while (retryCount > 0) {
                try {
                    System.out.println(e1.getMessage() + ", last " + retryCount + " times retry...");
                    response = client.post(url, body, headers, contentType);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    private String statWithRetry(String bucket, String fileKey, int retryCount) throws QiniuSuitsException {

        String fileInfoStr = "";

        try {
            fileInfoStr = JSONConvertUtils.toJson(bucketManager.stat(bucket, fileKey));
        } catch (QiniuException e1) {
            if (retryCount <= 0)
                throw new QiniuSuitsException(e1);
            while (retryCount > 0) {
                try {
                    System.out.println(e1.getMessage() + ", last " + retryCount + " times retry...");
                    fileInfoStr = JSONConvertUtils.toJson(bucketManager.stat(bucket, fileKey));
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return fileInfoStr;
    }

    public void closeBucketManager() {
        if (bucketManager != null)
            bucketManager.closeResponse();
    }
}