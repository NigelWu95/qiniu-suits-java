package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.model.FetchBody;
import com.qiniu.model.FetchFile;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AsyncFetch extends OperationBase implements IOssFileProcess, Cloneable {

    private String resultFileDir;
    private String processName;
    private FileReaderAndWriterMap fileReaderAndWriterMap = new FileReaderAndWriterMap();
    private FetchBody fetchBody;
    private boolean keepKey;
    private String keyPrefix;
    private M3U8Manager m3u8Manager;

    public AsyncFetch(Auth auth, Configuration configuration, FetchBody fetchBody, String resultFileDir,
                      String processName, int resultFileIndex)
            throws IOException {
        super(auth, configuration);
        this.resultFileDir = resultFileDir;
        this.processName = processName;
        this.fetchBody = fetchBody;
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public AsyncFetch(Auth auth, Configuration configuration, FetchBody fetchBody, String resultFileDir,
                      String processName) throws IOException {
        this(auth, configuration, fetchBody, resultFileDir, processName, 0);
    }

    public AsyncFetch getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        AsyncFetch asyncFetch = (AsyncFetch)super.clone();
        asyncFetch.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            asyncFetch.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return asyncFetch;
    }

    public String getProcessName() {
        return this.processName;
    }

    public String run(FetchBody fetchBody, int retryCount) throws QiniuException {

        Response response = fetchWithRetry(fetchBody, retryCount);
        if (response == null) return null;
        String responseBody = response.bodyString();
        int statusCode = response.statusCode;
        String reqId = response.reqId;
        response.close();

        return statusCode + "\t" + reqId + "\t" + responseBody;
    }

    public Response fetchWithRetry(FetchBody fetchBody, int retryCount)
            throws QiniuException {

        Response response = null;
        FetchFile fetchFile = fetchBody.fetchFiles.get(0);
        try {
            response = fetchBody.hasCustomArgs() ?
                    bucketManager.asynFetch(fetchFile.url, fetchBody.bucket, fetchFile.key, fetchFile.md5,
                            fetchFile.etag, fetchBody.callbackUrl, fetchBody.callbackBody, fetchBody.callbackBodyType,
                            fetchBody.callbackHost, fetchBody.fileType) :
                    bucketManager.asynFetch(fetchFile.url, fetchBody.bucket, fetchFile.key);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("async fetch " + fetchFile.url + " to " + fetchBody.bucket + ":" + fetchFile.key
                            + " " + e1.error() + ", last " + retryCount + " times retry...");
                    response = bucketManager.asynFetch(fetchFile.url, fetchBody.bucket, fetchFile.key);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    public void processFile(List<FileInfo> fileInfoList, boolean batch, int retryCount) throws QiniuException {

        if (fileInfoList == null || fileInfoList.size() == 0) return;
        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.key).collect(Collectors.toList());

        if (batch) {
            List<String> resultList = new ArrayList<>();
            for (String key : keyList) {
                fetchBody.fetchFiles.add(new FetchFile(key, "", "", ""));
                String result = run(fetchBody, retryCount);
                if (!StringUtils.isNullOrEmpty(result)) resultList.add(result);
            }
            if (resultList.size() > 0) fileReaderAndWriterMap.writeSuccess(String.join("\n", resultList));
            return;
        }

        int times = fileInfoList.size()/1000 + 1;

//        if (keyList == null || keyList.size() == 0) return;
//        int times = keyList.size()/1000 + 1;
//        for (int i = 0; i < times; i++) {
//            List<String> processList = keyList.subList(1000 * i, i == times - 1 ? keyList.size() : 1000 * (i + 1));
//            if (processList.size() > 0) {
//                try {
//                    String result = bucketCopy.batchRun(srcBucket, tarBucket, processList, keyPrefix, false,
//                            retryCount);
//                    if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
//                } catch (QiniuException e) {
//                    fileReaderAndWriterMap.writeErrorOrNull(srcBucket + "\t" + tarBucket + "\t" + keyPrefix + "\t"
//                            + processList + "\t" + false + "\t" + e.error());
//                    if (!e.response.needRetry()) qiniuException = e;
//                    else e.response.close();
//                }
//            }
//        }
    }

//    public AsyncFetch(Auth auth, String targetBucket, String resultFileDir, String processName) throws IOException {
//        this.processName = processName;
//        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, null);
//    }
//
//    public AsyncFetch(Auth auth, String targetBucket, String resultFileDir, String processName, M3U8Manager m3u8Manager)
//            throws IOException {
//        this(auth, targetBucket, resultFileDir, processName);
//        this.m3u8Manager = m3u8Manager;
//    }

    private void fetchResult(String url, String key) throws QiniuException {
        try {
//            String fetchResult = asyncFetch.run(url, key, 0);
            String fetchResult = run(null, 0);
            fileReaderAndWriterMap.writeSuccess(fetchResult);
        } catch (QiniuException e) {
            fileReaderAndWriterMap.writeErrorOrNull(url + "," + key + "\t" + e.error());
            if (!e.response.needRetry()) throw e;
            else e.response.close();
        }
    }

//    public void processItem(String source, String item) {
//        processItem(source, item, item);
//    }

//    public void processItem(String source, String item, String key) {
//        String url = source.endsWith("/") ? source + item : source + "/" + item;
//        fetchResult(url, key);
//    }
//
//    public void processItem(Auth auth, String source, String item) {
//        processItem(auth, source, item, item);
//    }
//
//    public void processItem(Auth auth, String source, String item, String key) {
//        String url = auth.privateDownloadUrl(source + item);
//        fetchResult(url, key);
//    }
//
//    public void processUrl(String url, String key) {
//        fetchResult(url, key);
//    }
//
//    public void processUrl(String url, String key, String format) {
//        processUrl(url, key);
//
//        if (Arrays.asList("hls", "HLS", "m3u8", "M3U8").contains(format)) {
//            fetchTSByM3U8(url);
//        }
//    }
//
//    public void processUrl(Auth auth, String url, String key) {
//        url = auth.privateDownloadUrl(url);
//        fetchResult(url, key);
//    }

//    public void processUrl(Auth auth, String url, String key, String format) {
//        url = auth.privateDownloadUrl(url);
//        processUrl(url, key);
//
//        if (Arrays.asList("hls", "HLS", "m3u8", "M3U8").contains(format)) {
//            fetchTSByM3U8(url);
//        }
//    }
//
//    private void fetchTSByM3U8(String rootUrl, String m3u8FilePath) {
//        List<VideoTS> videoTSList = new ArrayList<>();
//
//        try {
//            videoTSList = m3u8Manager.getVideoTSListByFile(rootUrl, m3u8FilePath);
//        } catch (IOException ioException) {
//            fileReaderAndWriterMap.writeOther("list ts failed: " + m3u8FilePath);
//        }
//
//        for (VideoTS videoTS : videoTSList) {
//            processUrl(videoTS.getUrl(), videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1]);
//        }
//    }
//
//    private void fetchTSByM3U8(String m3u8Url) {
//        List<VideoTS> videoTSList = new ArrayList<>();
//
//        try {
//            videoTSList = m3u8Manager.getVideoTSListByUrl(m3u8Url);
//        } catch (IOException ioException) {
//            fileReaderAndWriterMap.writeErrorOrNull("list ts failed: " + m3u8Url);
//        }
//
//        for (VideoTS videoTS : videoTSList) {
//            processUrl(videoTS.getUrl(), videoTS.getUrl().split("(https?://[^\\s/]+\\.[^\\s/\\.]{1,3}/)|(\\?ver=)")[1]);
//        }
//    }

    public void closeResource() {
        fileReaderAndWriterMap.closeWriter();
    }
}
