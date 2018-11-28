package com.qiniu.custom.fantx;

import com.qiniu.common.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileCopy implements ILineProcess<Map<String, String>>, Cloneable {

    private Auth auth;
    private Configuration configuration;
    private BucketManager bucketManager;
    private String bucket;
    private String toBucket;
    private String processName;
    private boolean batch = true;
    private volatile BatchOperations batchOperations;
    private int retryCount = 3;
    private String resultFileDir;
    private FileMap fileMap;

    private void initBaseParams(String toBucket) {
        this.processName = "copy";
        this.toBucket = toBucket;
    }

    public FileCopy(Auth auth, Configuration configuration, String fromBucket, String toBucket, String resultFileDir) {
        initBaseParams(toBucket);
        this.auth = auth;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(auth, configuration);
        this.bucket = fromBucket;
        this.batchOperations = new BatchOperations();
        this.resultFileDir = resultFileDir;
        this.fileMap = new FileMap();
    }

    public FileCopy(Auth auth, Configuration configuration, String fromBucket, String toBucket, String resultFileDir,
                    int resultFileIndex) throws IOException {
        this(auth, configuration, fromBucket, toBucket, resultFileDir);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public FileCopy getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        FileCopy fileCopy = (FileCopy)super.clone();
        fileCopy.bucketManager = new BucketManager(auth, configuration);
        fileCopy.batchOperations = new BatchOperations();
        fileCopy.fileMap = new FileMap();
        try {
            fileCopy.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return fileCopy;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getProcessName() {
        return processName;
    }

    public String getInfo() {
        return bucket + "\t" + toBucket;
    }

    public List<String> singleRun(List<Map<String, String>> lineList) throws QiniuException {

        List<String> resultList = new ArrayList<>();
        for (Map<String, String> copyLine : lineList) {
            try {
                Response response = null;
                try {
                    response = bucketManager.copy(bucket, copyLine.get("1"), toBucket, copyLine.get("0"), false);
                } catch (QiniuException e1) {
                    HttpResponseUtils.checkRetryCount(e1, retryCount);
                    while (retryCount > 0) {
                        try {
                            response = bucketManager.copy(bucket, copyLine.get("1"), toBucket, copyLine.get("0"), false);
                            retryCount = 0;
                        } catch (QiniuException e2) {
                            retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                        }
                    }
                }
                String result = HttpResponseUtils.getResult(response);
                if (!StringUtils.isNullOrEmpty(result)) resultList.add(result);
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, processName, getInfo() + "\t" + copyLine.toString());
            }
        }

        return resultList;
    }

    public List<String> batchRun(List<Map<String, String>> lineList) throws QiniuException {

        List<String> resultList = new ArrayList<>();
        int times = lineList.size()/1000 + 1;
        for (int i = 0; i < times; i++) {
            List<Map<String, String>> processList = lineList.subList(1000 * i, i == times - 1 ?
                    lineList.size() : 1000 * (i + 1));
            if (processList.size() > 0) {
                try {
                    Response response = null;
                    processList.forEach(copyLine ->
                            batchOperations.addCopyOp(bucket, copyLine.get("1"), toBucket, copyLine.get("0")));
                    try {
                        response = bucketManager.batch(batchOperations);
                    } catch (QiniuException e) {
                        HttpResponseUtils.checkRetryCount(e, retryCount);
                        while (retryCount > 0) {
                            try {
                                response = bucketManager.batch(batchOperations);
                                retryCount = 0;
                            } catch (QiniuException e1) {
                                retryCount = HttpResponseUtils.getNextRetryCount(e1, retryCount);
                            }
                        }
                    }
                    batchOperations.clearOps();
                    String result = HttpResponseUtils.getResult(response);
                    if (!StringUtils.isNullOrEmpty(result)) resultList.add(result);
                } catch (QiniuException e) {
                    HttpResponseUtils.processException(e, fileMap, processName, getInfo() + "\t" + String
                            .join(",", processList.stream().map(Object::toString).collect(Collectors.toList())));
                }
            }
        }
        return resultList;
    }

    public BatchOperations getOperations(List<Map<String, String>> lineList) throws QiniuException {

        return batchOperations;
    }

    public void processLine(List<Map<String, String>> list) throws QiniuException {
        List<String> resultList = batch ? batchRun(list) : singleRun(list);
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
