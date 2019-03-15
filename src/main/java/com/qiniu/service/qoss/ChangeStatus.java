package com.qiniu.service.qoss;

import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.FileNameUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ChangeStatus extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    final private int status;
    final private String rmPrefix;

    public ChangeStatus(String accessKey, String secretKey, Configuration configuration, String bucket, int status,
                        String rmPrefix, String savePath, int saveIndex) throws IOException {
        super("status", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
        this.status = status;
        this.rmPrefix = rmPrefix;
    }

    public ChangeStatus(String accessKey, String secretKey, Configuration configuration, String bucket, int status,
                        String rmPrefix, String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, status, rmPrefix, savePath, 0);
    }

    synchronized public List<Map<String, String>> setBatchOperations(List<Map<String, String>> lineList) {
        batchOperations.clearOps();
        return lineList.parallelStream().filter(line -> {
            try {
                batchOperations.addChangeStatusOps(bucket, status, FileNameUtils.rmPrefix(rmPrefix, line.get("key")));
                return true;
            } catch (IOException e) {
                errorLineList.add(String.valueOf(line) + "\t" + e.getMessage());
                return false;
            }
        }).collect(Collectors.toList());
    }

    public String getInputParams(Map<String, String> line) {
        return line.get("key");
    }
}
