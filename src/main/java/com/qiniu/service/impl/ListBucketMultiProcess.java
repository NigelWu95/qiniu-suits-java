package com.qiniu.service.impl;

import com.qiniu.common.*;
import com.qiniu.common.QiniuBucketManager.*;
import com.qiniu.config.PropertyConfig;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.service.oss.ListBucketProcessor;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.UrlSafeBase64;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListBucketMultiProcess {

    public static void main(String[] args) throws IOException {
        FileReaderAndWriterMap targetFileReaderAndWriterMap = new FileReaderAndWriterMap();
        targetFileReaderAndWriterMap.initOutputStreamWriter(System.getProperty("user.home") + "/Downloads/test/", "list");
        PropertyConfig propertyConfig = new PropertyConfig(".qiniu.properties");
        QiniuAuth auth = QiniuAuth.create(propertyConfig.getProperty("user_access_key"),  propertyConfig.getProperty("user_secret_key"));
        QiniuBucketManager bucketManager = new QiniuBucketManager(auth, new Configuration(Zone.autoZone()));
        ListBucketMultiProcess listBucketMultiProcessor = new ListBucketMultiProcess(bucketManager, targetFileReaderAndWriterMap);
        listBucketMultiProcessor.doMultiList("temp", null);
    }

    private ListBucketProcessor listBucketProcessor;
    private QiniuBucketManager bucketManager;

    public ListBucketMultiProcess(QiniuBucketManager bucketManager, FileReaderAndWriterMap fileReaderAndWriterMap) {
        this.bucketManager = bucketManager;
        this.listBucketProcessor = ListBucketProcessor.getChangeStatusProcessor(bucketManager, fileReaderAndWriterMap);
    }

    public void doMultiList(String bucket, IOssFileProcess iOssFileProcessor) throws QiniuException {
        List<String> prefixArray = new ArrayList<>();
        List<String> prefixs = Arrays.asList("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".split(""));
        prefixArray.add(0, "");
        prefixArray.addAll(prefixs);

        List<String> firstKeyList = new ArrayList<>();
        for (int i = 0; i < prefixArray.size(); i++) {
            FileListing fileListing = bucketManager.listFiles(bucket, prefixArray.get(i), null, 1, null);
            FileInfo[] items = fileListing.items;

            for (FileInfo fileInfo : items) {
                firstKeyList.add(0, fileInfo.key);
            }
        }

        for (int i = 0; i < firstKeyList.size(); i++) {
            String endFileKey = i == firstKeyList.size() - 1 ? "" : firstKeyList.get(i + 1);
            String startMarker = UrlSafeBase64.encodeToString("{\"c\":0,\"k\":\"" + firstKeyList.get(i) + "\"}");
            FileListing fileListing = bucketManager.listFiles(bucket, prefixArray.get(i), startMarker, 1000, null);
        }
    }
}