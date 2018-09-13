package com.qiniu.examples;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.qiniu.common.*;
import com.qiniu.service.auvideo.M3U8Manager;
import com.qiniu.interfaces.IUrlItemProcess;
import com.qiniu.service.impl.BucketCopyItemProcess;
import com.qiniu.service.impl.FetchUrlItemProcess;
import com.qiniu.service.impl.NothingUrlItemProcess;
import com.qiniu.service.jedi.VideoExport;
import com.qiniu.service.jedi.VideoManage;
import com.qiniu.config.PropertyConfig;
import com.qiniu.storage.Configuration;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VideoExportMain {

    public static void main(String[] args) {

        PropertyConfig propertyConfig = new PropertyConfig(".qiniu.properties");
        String ak = propertyConfig.getProperty("access_key");
        String sk = propertyConfig.getProperty("secret_key");
        String u_ak = propertyConfig.getProperty("user_access_key");
        String u_sk = propertyConfig.getProperty("user_secret_key");
        String jediResultBucket = propertyConfig.getProperty("jedi_result");
        String bucket = propertyConfig.getProperty("bucket");
        String jediHub = propertyConfig.getProperty("jedi_hub");
        String targetFileDir = System.getProperty("user.home") + "/Downloads/test/";
        QiniuAuth u_auth = QiniuAuth.create(u_ak, u_sk);
        QiniuAuth auth = QiniuAuth.create(ak, sk);
        VideoExport videoExport = new VideoExport();
        Map<String, Object> map = videoExport.getFirstResult(auth, jediHub);
        System.out.println(map);
        VideoExportMain videoExportMain = new VideoExportMain();
        IUrlItemProcess processor = null;

        try {
            processor = new NothingUrlItemProcess();
            processor = videoExportMain.getBucketCopyProcess(u_auth, jediResultBucket, bucket, targetFileDir);
//            processor = videoExportMain.getFetchProcess(u_auth, bucket, targetFileDir);

            // isBiggerThan 标志为 true 时，在 pointTime 时间点之前的记录进行处理，isBiggerThan 标志为 false 时，在 pointTime 时间点之后的记录进行处理。
            videoExport.setPointTime("2018-09-11 00:00:00", true);

            videoExportMain
//                    .exportItems(auth, videoExport, jediHub, targetFileDir, processor);
                    .multiExportItems(auth, videoExport, jediHub, targetFileDir, processor);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            processor.close();
        }
    }

    public IUrlItemProcess getBucketCopyProcess(QiniuAuth auth, String srcBucket, String tarBucket, String targetFileDir) throws QiniuSuitsException, IOException {
        FileReaderAndWriterMap targetFileReaderAndWriterMap = new FileReaderAndWriterMap();
        targetFileReaderAndWriterMap.initOutputStreamWriter(targetFileDir, "copy");
        M3U8Manager m3u8Manager = new M3U8Manager();
        QiniuBucketManager bucketManager = new QiniuBucketManager(auth, new Configuration(Zone.autoZone()));
        IUrlItemProcess processor = new BucketCopyItemProcess(bucketManager, srcBucket, tarBucket, "video/", targetFileReaderAndWriterMap, m3u8Manager);
        return processor;
    }

    public IUrlItemProcess getFetchProcess(QiniuAuth auth, String bucket, String targetFileDir) throws QiniuSuitsException, IOException {
        FileReaderAndWriterMap targetFileReaderAndWriterMap = new FileReaderAndWriterMap();
        targetFileReaderAndWriterMap.initOutputStreamWriter(targetFileDir, "fetch");
        M3U8Manager m3u8Manager = new M3U8Manager();
        IUrlItemProcess processor = new FetchUrlItemProcess(auth, bucket, targetFileReaderAndWriterMap, m3u8Manager);
        return processor;
    }

    public void exportItems(QiniuAuth auth, final VideoExport videoExport, String jediHub, String targetFileDir, final IUrlItemProcess processor) throws IOException {
        Gson gson = new Gson();
        int count = 500;
        long total = 0;
        Map<String, Object> result = null;
        JsonObject jsonObject = null;
        String cursor = null;
        JsonArray jsonElements = null;
        total = videoExport.getTotalCount(auth, jediHub);
        System.out.println("count: " + total);
        System.out.println("exporter started...");
        VideoManage vm = new VideoManage(auth);
        FileReaderAndWriterMap targetFileReaderAndWriterMap = new FileReaderAndWriterMap();
        targetFileReaderAndWriterMap.initOutputStreamWriter(targetFileDir, "export");

        for (int i = 0; i < total/count + 1; i++) {
            result = vm.getVideoInfoList(jediHub, cursor, count);
            jsonObject = gson.fromJson((String) result.get("msg"), JsonObject.class);
            cursor = jsonObject.get("cursor") == null ? "" : jsonObject.get("cursor").getAsString();
            jsonElements = jsonObject.getAsJsonArray("items");
            videoExport.processUrlGroupbyFormat(targetFileReaderAndWriterMap, jsonElements, processor);
        }

        targetFileReaderAndWriterMap.closeStreamWriter();
        System.out.println("export completed for: " + targetFileDir);
    }

    public void multiExportItems(QiniuAuth auth, final VideoExport videoExport, String jediHub, String targetFileDir, final IUrlItemProcess processor) throws IOException {
        Gson gson = new Gson();
        int count = 500;
        long total = 0;
        Map<String, Object> result = null;
        JsonObject jsonObject = null;
        String cursor = null;
        JsonArray jsonElements = null;

        // 开启线程池，设置线程个数
        ExecutorService executorPool = Executors.newFixedThreadPool(5);
        VideoManage vm = new VideoManage(auth);
        FileReaderAndWriterMap targetFileReaderAndWriterMap = new FileReaderAndWriterMap();
        targetFileReaderAndWriterMap.initOutputStreamWriter(targetFileDir, "export");
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
            executorPool.execute(new Runnable() {
                public void run() {
                    videoExport.processUrlGroupbyFormat(finalTargetFileReaderAndWriterMap, finalJsonElements, processor);
                }
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

        finalTargetFileReaderAndWriterMap.closeStreamWriter();
        targetFileReaderAndWriterMap.closeStreamWriter();
        System.out.println("export completed for: " + targetFileDir);
    }
}