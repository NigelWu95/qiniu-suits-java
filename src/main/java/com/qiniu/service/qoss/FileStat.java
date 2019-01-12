package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FileStat extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultPath, int resultIndex)
            throws IOException {
        super(auth, configuration, bucket, "stat", resultPath, resultIndex);
    }

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultPath) throws IOException {
        this(auth, configuration, bucket, resultPath, 0);
    }

    public String processLine(Map<String, String> line) throws QiniuException {
        FileInfo result = bucketManager.stat(bucket, line.get("key"));
        return JsonConvertUtils.toJsonWithoutUrlEscape(result);
    }

    synchronized public BatchOperations getOperations(List<Map<String, String>> lineList) {
        lineList.forEach(line -> {
            if (StringUtils.isNullOrEmpty(line.get("key")))
                errorLineList.add(String.valueOf(line) + "\tno target key in the line map.");
            else
                batchOperations.addStatOps(bucket, line.get("key"));
        });
        return batchOperations;
    }

    public String getInputParams(Map<String, String> line) {
        return line.get("key");
    }
}
