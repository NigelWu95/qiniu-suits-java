package com.qiniu.service.oss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.*;
import com.qiniu.service.interfaces.IOssFileProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.List;

public class CopyFile extends OperationBase implements IOssFileProcess, Cloneable {

    private String toBucket;
    private boolean keepKey;
    private String keyPrefix;

    public CopyFile(Auth auth, Configuration configuration, String fromBucket, String toBucket,
                    boolean keepKey, String keyPrefix, String resultFileDir, String processName,
                    int resultFileIndex) throws IOException {
        super(auth, configuration, fromBucket, resultFileDir, processName, resultFileIndex);
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

    protected Response getResponse(String key) throws QiniuException {
        return bucketManager.copy(bucket, key, toBucket, keepKey ? keyPrefix + key : null, false);
    }

    synchronized protected BatchOperations getOperations(List<String> keys) {
        if (keepKey) {
            keys.forEach(fileKey -> batchOperations.addCopyOp(bucket, fileKey, toBucket,
                    keyPrefix + fileKey));
        } else {
            keys.forEach(fileKey -> batchOperations.addCopyOp(bucket, fileKey, toBucket, null));
        }

        return batchOperations;
    }

    protected String getInfo() {
        return bucket + "\t" + toBucket + "\t" + keyPrefix;
    }
}
