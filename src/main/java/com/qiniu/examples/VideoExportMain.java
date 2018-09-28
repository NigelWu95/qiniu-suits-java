package com.qiniu.examples;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.qiniu.common.*;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.interfaces.IUrlItemProcess;
import com.qiniu.service.impl.BucketCopyProcess;
import com.qiniu.service.impl.AsyncFetchProcess;
import com.qiniu.service.jedi.VideoExport;
import com.qiniu.service.jedi.VideoManage;
import com.qiniu.config.PropertyConfig;
import com.qiniu.storage.Configuration;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VideoExportMain {

    public static void main(String[] args) throws Exception{

        PropertyConfig propertyConfig = new PropertyConfig(".qiniu.properties");
        String ak = propertyConfig.getProperty("access_key");
        String sk = propertyConfig.getProperty("secret_key");
        String jediHub = propertyConfig.getProperty("jedi_hub");
        String jediResultBucket = propertyConfig.getProperty("jedi_result");
        String bucket = propertyConfig.getProperty("to_bucket");
        String targetFileDir = System.getProperty("user.home") + "/Downloads/test/";
        QiniuAuth auth = QiniuAuth.create(ak, sk);
        VideoExport videoExport = new VideoExport();
        Map<String, Object> map = videoExport.getFirstResult(auth, jediHub);
        System.out.println(map);
        VideoExportMain videoExportMain = new VideoExportMain();
        IUrlItemProcess processor = null;

        try {
            processor = videoExportMain.getBucketCopyProcess(auth, jediResultBucket, bucket, targetFileDir);
//            processor = videoExportMain.getFetchProcess(auth, bucket, targetFileDir);

            // isBiggerThan 标志为 true 时，在 pointTime 时间点之前的记录进行处理，isBiggerThan 标志为 false 时，在 pointTime 时间点之后的记录进行处理。
            videoExport.setPointTime("2018-09-11 00:00:00", true);

            videoExportMain
//                    .exportItems(auth, videoExport, jediHub, targetFileDir, processor);
                    .multiExportItems(auth, videoExport, jediHub, targetFileDir, processor);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (processor != null)
                processor.closeResource();
        }
    }

    public IUrlItemProcess getBucketCopyProcess(QiniuAuth auth, String srcBucket, String tarBucket, String targetFileDir) throws IOException {
        M3U8Manager m3u8Manager = new M3U8Manager();
        IUrlItemProcess processor = new BucketCopyProcess(auth, new Configuration(Zone.autoZone()), srcBucket, tarBucket, "video/", targetFileDir, m3u8Manager);
        return processor;
    }

    public IUrlItemProcess getFetchProcess(QiniuAuth auth, String bucket, String targetFileDir) throws IOException {
        M3U8Manager m3u8Manager = new M3U8Manager();
        IUrlItemProcess processor = new AsyncFetchProcess(auth, bucket, targetFileDir, m3u8Manager);
        return processor;
    }

    public void exportItems(QiniuAuth auth, final VideoExport videoExport, String jediHub, String targetFileDir, final IUrlItemProcess processor) throws IOException {
        Gson gson = new Gson();
        int count = 500;
        long total;
        Map<String, Object> result;
        JsonObject jsonObject;
        String cursor = null;
        JsonArray jsonElements;
        total = videoExport.getTotalCount(auth, jediHub);
        System.out.println("count: " + total);
        System.out.println("exporter started...");
        VideoManage vm = new VideoManage(auth);
        FileReaderAndWriterMap targetFileReaderAndWriterMap = new FileReaderAndWriterMap();
        targetFileReaderAndWriterMap.initWriter(targetFileDir, "export");

        for (int i = 0; i < total/count + 1; i++) {
            result = vm.getVideoInfoList(jediHub, cursor, count);
            jsonObject = gson.fromJson((String) result.get("msg"), JsonObject.class);
            cursor = jsonObject.get("cursor") == null ? "" : jsonObject.get("cursor").getAsString();
            jsonElements = jsonObject.getAsJsonArray("items");
            videoExport.processUrlGroupbyFormat(targetFileReaderAndWriterMap, jsonElements, processor);
        }

        targetFileReaderAndWriterMap.closeWriter();
        System.out.println("export completed for: " + targetFileDir);
    }

    public void multiExportItems(QiniuAuth auth, final VideoExport videoExport, String jediHub, String targetFileDir, final IUrlItemProcess processor) throws IOException {
        Gson gson = new Gson();
        int count = 500;
        long total;
        Map<String, Object> result;
        JsonObject jsonObject;
        String cursor = null;
        JsonArray jsonElements;

        // 开启线程池，设置线程个数
        ExecutorService executorPool = Executors.newFixedThreadPool(5);
        VideoManage vm = new VideoManage(auth);
        FileReaderAndWriterMap targetFileReaderAndWriterMap = new FileReaderAndWriterMap();
        targetFileReaderAndWriterMap.initWriter(targetFileDir, "export");
        final FileReaderAndWriterMap finalTargetFileReaderAndWriterMap = targetFileReaderAndWriterMap;
        total = videoExport.getTotalCount(auth, jediHub);
        System.out.println("count: " + total);
        System.out.println("exporter started...");

        for (int i = 0; i < total/count + 1; i++) {
            result = vm.getVideoInfoList(jediHub, cursor, count);
            jsonObject = gson.fromJson((String) result.get("msg"), JsonObject.class);
            cursor = jsonObject.get("cursor") == null ? "" : jsonObject.get("cursor").getAsString();
            jsonElements = jsonObject.getAsJsonArray("items");
            final JsonArray finalJsonElements = jsonElements;
            executorPool.execute(() -> {
                    videoExport.processUrlGroupbyFormat(finalTargetFileReaderAndWriterMap, finalJsonElements, processor);
            });
        }

        executorPool.shutdown();

        try {
            while (!executorPool.isTerminated()) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        finalTargetFileReaderAndWriterMap.closeWriter();
        targetFileReaderAndWriterMap.closeWriter();
        System.out.println("export completed for: " + targetFileDir);
    }
}