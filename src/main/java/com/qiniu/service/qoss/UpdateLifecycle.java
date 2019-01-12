package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class UpdateLifecycle extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    final private int days;

    public UpdateLifecycle(Auth auth, Configuration configuration, String bucket, int days, String resultPath,
                           int resultIndex) throws IOException {
        super(auth, configuration, bucket, "lifecycle", resultPath, resultIndex);
        this.days = days;
    }

    public UpdateLifecycle(Auth auth, Configuration configuration, String bucket, int days, String resultPath)
            throws IOException {
        this(auth, configuration, bucket, days, resultPath, 0);
    }

    public String processLine(Map<String, String> line) throws QiniuException {
        Response response = bucketManager.deleteAfterDays(bucket, line.get("key"), days);
        return response.statusCode + "\t" + HttpResponseUtils.getResult(response);
    }

    synchronized public BatchOperations getOperations(List<Map<String, String>> lineList) {
        lineList.forEach(line -> {
            if (StringUtils.isNullOrEmpty(line.get("key")))
                errorLineList.add(String.valueOf(line) + "\tno target key in the line map.");
            else
                batchOperations.addDeleteAfterDaysOps(bucket, days, line.get("key"));
        });
        return batchOperations;
    }

    public String getInputParams(Map<String, String> line) {
        return line.get("key");
    }
}
