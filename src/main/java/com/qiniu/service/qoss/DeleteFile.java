package com.qiniu.service.qoss;

import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.sdk.BucketManager.BatchOperations;
import com.qiniu.service.interfaces.IQossProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class DeleteFile extends OperationBase implements IQossProcess, Cloneable {

    private void initBaseParams() {
        this.processName = "delete";
    }

    public DeleteFile(Auth auth, Configuration configuration, String fromBucket, String resultFileDir,
                      int resultFileIndex) throws IOException {
        super(auth, configuration, fromBucket, resultFileDir);
        initBaseParams();
        this.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public DeleteFile(Auth auth, Configuration configuration, String fromBucket, String resultFileDir) {
        super(auth, configuration, fromBucket, resultFileDir);
        initBaseParams();
    }

    public DeleteFile getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        DeleteFile copyFile = (DeleteFile)super.clone();
        copyFile.fileReaderAndWriterMap = new FileReaderAndWriterMap();
        try {
            copyFile.fileReaderAndWriterMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return copyFile;
    }

    public String getInfo() {
        return bucket;
    }

    protected Response getResponse(FileInfo fileInfo) throws QiniuException {
        return bucketManager.delete(bucket, fileInfo.key);
    }

    synchronized protected BatchOperations getOperations(List<FileInfo> fileInfoList) {

        List<String> keyList = fileInfoList.stream().map(fileInfo -> fileInfo.key).collect(Collectors.toList());
        batchOperations.addDeleteOp(bucket, keyList.toArray(new String[]{}));
        return batchOperations;
    }
}
