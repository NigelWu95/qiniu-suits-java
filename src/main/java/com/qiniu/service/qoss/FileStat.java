package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileStat extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultPath, int resultIndex)
            throws IOException {
        super(auth, configuration, bucket, "stat", resultPath, resultIndex);
    }

    public FileStat(Auth auth, Configuration configuration, String bucket, String resultPath) throws IOException {
        this(auth, configuration, bucket, resultPath, 0);
    }

    protected String processLine(Map<String, String> line) throws QiniuException {
        FileInfo result = bucketManager.stat(bucket, line.get("key"));
        return JsonConvertUtils.toJsonWithoutUrlEscape(result);
    }

    synchronized protected BatchOperations getOperations(List<Map<String, String>> lineList) {
        List<String> keyList = lineList.stream().map(line -> line.get("key")).collect(Collectors.toList());
        return batchOperations.addStatOps(bucket, keyList.toArray(new String[]{}));
    }
}
