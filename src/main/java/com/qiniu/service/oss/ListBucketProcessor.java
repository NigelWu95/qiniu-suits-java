package com.qiniu.service.oss;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.qiniu.common.*;
import com.qiniu.common.QiniuBucketManager.*;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.JSONConvertUtils;
import com.qiniu.util.StringMap;
import com.qiniu.util.StringUtils;
import com.qiniu.util.UrlSafeBase64;

import java.io.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class ListBucketProcessor {

    private QiniuAuth auth;
    private QiniuBucketManager bucketManager;
    private Client client;
    private FileReaderAndWriterMap targetFileReaderAndWriterMap;

    private static volatile ListBucketProcessor listBucketProcessor = null;

    public ListBucketProcessor(QiniuAuth auth, Configuration configuration, FileReaderAndWriterMap targetFileReaderAndWriterMap) {
        this.auth = auth;
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
    public String doFileList(String bucket, String prefix, String marker, String endFile, int limit, IOssFileProcess iOssFileProcessor) {

        String endMarker = null;

        try {
            FileListing fileListing = bucketManager.listFiles(bucket, prefix, marker, limit, null);
            FileInfo[] items = fileListing.items;
            String fileInfoStr = getFileListingInfo(bucket, fileListing, 0);
            targetFileReaderAndWriterMap.writeSuccess(fileInfoStr);
            if (iOssFileProcessor != null) {
                iOssFileProcessor.processFile(fileInfoStr);
            }

            for (int i = 1; i < items.length; i++) {
                if (items[i].key.equals(endFile)) {
                    fileListing = null;
                    break;
                }
                fileInfoStr = JSONConvertUtils.toJson(items[i]);
                targetFileReaderAndWriterMap.writeSuccess(fileInfoStr);
                if (iOssFileProcessor != null) {
                    iOssFileProcessor.processFile(fileInfoStr);
                }
            }

            endMarker = fileListing == null ? null : fileListing.marker;
        } catch (QiniuException e) {
            targetFileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefix + "\t" + limit + "{\"msg\":\"" + e.error() + "\"}");
        } catch (QiniuSuitsException e) {
            targetFileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefix + "\t" + limit + "\t" + e.getMessage());
        }

        return endMarker;
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
                    iOssFileProcessor.processFile(JSONConvertUtils.toJson(fileInfo));
                }
            }
        }
    }

    /*
    v2 的 list 接口，接收到响应后通过 java8 的流来处理响应的文本流。
     */
    public String doFileListV2(String bucket, String prefix, String delimiter, String marker, String endFile, int limit,
                               IOssFileProcess iOssFileProcessor, boolean withParallel) {

        Response response = null;
        InputStream inputStream;
        Reader reader;
        BufferedReader bufferedReader;
        AtomicBoolean endFlag = new AtomicBoolean(false);
        AtomicReference<String> endMarker = new AtomicReference<>();

        try {
            response = listV2(bucket, prefix, delimiter, marker, limit);
            inputStream = new BufferedInputStream(response.bodyStream());
            reader = new InputStreamReader(inputStream);
            bufferedReader = new BufferedReader(reader);
            Stream<String> lineStream = withParallel ? bufferedReader.lines().parallel() : bufferedReader.lines();
            lineStream.forEach(line -> {
                        try {
                            String fileInfoStr = getFileInfoV2(bucket, line);
                            if (endFile.equals(JSONConvertUtils.toJson(fileInfoStr).get("key").getAsString())) {
                                endFlag.set(true);
                                endMarker.set(null);
                            }
                            if (!endFlag.get()) {
                                targetFileReaderAndWriterMap.writeSuccess(fileInfoStr);
                                iOssFileProcessor.processFile(fileInfoStr);
                                endMarker.set(JSONConvertUtils.toJson(line).get("marker").getAsString());
                            }
                        } catch (QiniuException e) {
                            targetFileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + line + "\t" + "{\"msg\":\"" + e.error() + "\"}");
                        } catch (QiniuSuitsException e) {
                            targetFileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + line + "\t" + e.getMessage());
                        }
                    });
            inputStream.close();
            reader.close();
            bufferedReader.close();
        } catch (QiniuException e) {
            targetFileReaderAndWriterMap.writeErrorAndNull(bucket + "\t" + prefix + "\t" + limit + "\t" + "{\"msg\":\"" + e.error() + "\"}");
        } catch (IOException e) {
            targetFileReaderAndWriterMap.writeOther(bucket + "\t" + prefix + "\t" + limit + "\t" + "{\"mdg\":\"io error\"}");
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
    public Response listV2(String bucket, String prefix, String delimiter, String marker, int limit) throws QiniuException {

        String prefixParam = StringUtils.isNullOrEmpty(prefix) ? "" : "&prefix=" + prefix;
        String delimiterParam = StringUtils.isNullOrEmpty(delimiter) ? "" : "&delimiter=" + delimiter;
        String limitParam = limit == 0 ? "" : "&limit=" + limit;
        String markerParam = StringUtils.isNullOrEmpty(marker) ? "" : "&marker=" + marker;
        String url = "http://rsf.qbox.me/v2/list?bucket=" + bucket + prefixParam + delimiterParam + limitParam + markerParam;
        String authorization = "QBox " + auth.signRequest(url, null, null);
        StringMap headers = new StringMap().put("Authorization", authorization);
        return client.post(url, null, headers, null);
    }

    public String getFileListingInfo(String bucket, FileListing fileListing, int index) throws QiniuSuitsException, QiniuException {
        if (fileListing == null)
            throw new QiniuSuitsException("line is empty");

        return getFileInfo(bucket, fileListing.items, fileListing.marker, index);
    }

    public String getFileInfo(String bucket, FileInfo[] items, String marker, int index) throws QiniuSuitsException, QiniuException {
        return items.length == 0 ? getFileInfoByMarker(bucket, marker) : JSONConvertUtils.toJson(items[index]);
    }

    public String getFileInfoByMarker(String bucket, String marker)throws QiniuSuitsException, QiniuException {
        String fileKey;
        String fileInfoStr;

        if (StringUtils.isNullOrEmpty(marker)) {
            JsonObject decodedMarker = JSONConvertUtils.toJson(new String(UrlSafeBase64.decode(marker)));
            fileKey = decodedMarker.get("k").getAsString();
            fileInfoStr = JSONConvertUtils.toJson(bucketManager.stat(bucket, fileKey));
        } else
            throw new QiniuSuitsException("marker is empty");

        return fileInfoStr;
    }

    public String getFileInfoV2(String bucket, String line) throws QiniuSuitsException, QiniuException {
        String fileKey;
        String fileInfoStr;

        if (StringUtils.isNullOrEmpty(line))
            throw new QiniuSuitsException("line is empty");
        JsonObject json = JSONConvertUtils.toJson(line);
        JsonElement jsonElement = json.get("item");
        if (jsonElement == null || "null".equals(jsonElement.toString())) {
            if (json.get("marker") != null && json.get("marker").getAsString().equals("")) {
                JsonObject decodedMarker = JSONConvertUtils.toJson(new String(UrlSafeBase64.decode(json.get("marker").getAsString())));
                fileKey = decodedMarker.get("k").getAsString();
                fileInfoStr = JSONConvertUtils.toJson(bucketManager.stat(bucket, fileKey));
            } else
                throw new QiniuSuitsException("marker is empty");
        } else {
            fileInfoStr = JSONConvertUtils.toJson(json.getAsJsonObject("item"));
        }

        return fileInfoStr;
    }

    public void closeBucketManager() {
        if (bucketManager != null)
            bucketManager.closeResponse();
    }
}