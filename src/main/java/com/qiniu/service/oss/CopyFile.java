package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
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

public class CopyFile extends OperationBase implements IOssFileProcess, Cloneable {

    private String fromBucket;
    private String toBucket;
    private boolean keepKey;
    private String keyPrefix;

    public CopyFile(Auth auth, Configuration configuration, String fromBucket, String toBucket,
                    boolean keepKey, String keyPrefix, String resultFileDir, String processName,
                    int resultFileIndex) throws IOException {
        super(auth, configuration, resultFileDir, processName, resultFileIndex);
        this.fromBucket = fromBucket;
        this.toBucket = toBucket;
        this.keepKey = keepKey;
        this.keyPrefix = StringUtils.isNullOrEmpty(keyPrefix) ? "" : keyPrefix;
    }

    public CopyFile(Auth auth, Configuration configuration, String srcBucket, String tarBucket,
                    boolean keepKey, String keyPrefix, String resultFileDir, String processName)
            throws IOException {
        this(auth, configuration, srcBucket, tarBucket, keepKey, keyPrefix, resultFileDir, processName, 0);
    }

    public CopyFile getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        CopyFile copyFile = (CopyFile)super.clone();
        copyFile.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            copyFile.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CloneNotSupportedException();
        }
        return copyFile;
    }

    public String getProcessName() {
        return this.processName;
    }

    public Response singleWithRetry(String key, int retryCount) throws QiniuException {

        Response response = null;
        try {
            response = bucketManager.copy(fromBucket, key, toBucket, keepKey ? keyPrefix + key : null, false);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    response = bucketManager.copy(fromBucket, key, toBucket, keyPrefix + key, false);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return response;
    }

    protected BatchOperations getOperations(List<String> keys) {
        if (keepKey) {
            keys.forEach(fileKey -> batchOperations.addCopyOp(fromBucket, fileKey, toBucket,
                    keyPrefix + fileKey));
        } else {
            keys.forEach(fileKey -> batchOperations.addCopyOp(fromBucket, fileKey, toBucket, null));
        }

        return batchOperations;
    }

    protected String getInfo() {
        return fromBucket + "\t" + toBucket + "\t" + keyPrefix;
    }

    public void processFile(List<FileInfo> fileInfoList, boolean batch, int retryCount) throws QiniuException {

        if (fileInfoList == null || fileInfoList.size() == 0) return;
        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.key).collect(Collectors.toList());

        if (batch) {
            List<String> resultList = new ArrayList<>();
            for (String key : keyList) {
                try {
                    String result = run(key, retryCount);
                    if (!StringUtils.isNullOrEmpty(result)) resultList.add(result);
                } catch (QiniuException e) {
                    System.out.println(processName + " failed. " + e.error());
                    fileReaderAndWriterMap.writeErrorOrNull(fromBucket + "\t" + toBucket + "\t" + keyPrefix + "\t"
                            + key + "\t" + "\t" + e.error());
                    if (!e.response.needRetry()) throw e;
                    else e.response.close();
                }
            }
            if (resultList.size() > 0) fileReaderAndWriterMap.writeSuccess(String.join("\n", resultList));
            return;
        }

        int times = fileInfoList.size()/1000 + 1;
        for (int i = 0; i < times; i++) {
            List<String> processList = keyList.subList(1000 * i, i == times - 1 ? keyList.size() : 1000 * (i + 1));
            if (processList.size() > 0) {
                try {
                    String result = batchRun(processList, retryCount);
                    if (!StringUtils.isNullOrEmpty(result)) fileReaderAndWriterMap.writeSuccess(result);
                } catch (QiniuException e) {
                    System.out.println("batch " + processName + " failed. " + e.error());
                    fileReaderAndWriterMap.writeErrorOrNull(fromBucket + "\t" + toBucket + "\t" + keyPrefix + "\t"
                            + processList + "\t" + "\t" + e.error());
                    if (!e.response.needRetry()) throw e;
                    else e.response.close();
                }
            }
        }
    }
}
