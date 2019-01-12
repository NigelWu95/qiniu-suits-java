package com.qiniu.service.qoss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.StorageType;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ChangeType extends OperationBase implements ILineProcess<Map<String, String>>, Cloneable {

    final private int type;

    public ChangeType(Auth auth, Configuration configuration, String bucket, int type, String resultPath,
                      int resultIndex) throws IOException {
        super(auth, configuration, bucket, "type", resultPath, resultIndex);
        this.type = type;
    }

    public ChangeType(Auth auth, Configuration configuration, String bucket, int type, String resultPath)
            throws IOException {
        this(auth, configuration, bucket, type, resultPath, 0);
    }

    public String processLine(Map<String, String> line) throws QiniuException {
        StorageType storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;
        Response response = bucketManager.changeType(bucket, line.get("key"), storageType);
        return "{\"code\":" + response.statusCode + ",\"message\":\"" + HttpResponseUtils.getResult(response) + "\"}";
    }

    synchronized public BatchOperations getOperations(List<Map<String, String>> lineList) {
        lineList.forEach(line -> {
            if (StringUtils.isNullOrEmpty(line.get("key")))
                errorLineList.add(String.valueOf(line) + "\tno target key in the line map.");
            else
                batchOperations.addChangeTypeOps(bucket, type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY,
                        line.get("key"));
        });
        return batchOperations;
    }

    public String getInputParams(Map<String, String> line) {
        return line.get("key");
    }
}
