package com.qiniu.process.qiniu;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.interfaces.IFileCaller;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.*;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.StorageType;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpRespUtils;
import com.qiniu.util.CloudApiUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ChangeType extends Base<Map<String, String>> {

    private StorageType storageType;
    private BatchOperations batchOperations;
    private List<Map<String, String>> lines;
    private Auth auth;
    private Configuration configuration;
    private BucketManager bucketManager;
    private int days;
    private String condition;
    private RestoreArchive restoreArchive;
    private IFileCaller<Map<String, String>> caller;

    public ChangeType(String accessKey, String secretKey, Configuration configuration, String bucket, int type)
            throws IOException {
        super("type", accessKey, secretKey, bucket);
        storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;
        this.auth = Auth.create(accessKey, secretKey);
        this.configuration = configuration;
        this.bucketManager = new BucketManager(auth, configuration); // BucketManager 中已经做了 configuration.clone()
        CloudApiUtils.checkQiniu(bucketManager, bucket);
        caller = list -> null;
    }

    public ChangeType(String accessKey, String secretKey, Configuration configuration, String bucket, int type,
                      String savePath, int saveIndex) throws IOException {
        super("type", accessKey, secretKey, bucket, savePath, saveIndex);
        storageType = type == 0 ? StorageType.COMMON : StorageType.INFREQUENCY;
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.lines = new ArrayList<>();
        this.configuration = configuration;
        this.auth = Auth.create(accessKey, secretKey);
        this.bucketManager = new BucketManager(auth, configuration);
        CloudApiUtils.checkQiniu(bucketManager, bucket);
        caller = list -> null;
    }

    public ChangeType(String accessKey, String secretKey, Configuration configuration, String bucket, int type,
                      String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, type, savePath, 0);
    }

    public void enableRestoreArchive(int days, String condition) {
        this.days = days;
        this.condition = condition;
        restoreArchive = new RestoreArchive(auth, secretKey, configuration, bucket, days, condition);
        caller = restoreArchive::batchResult;
        if (batchSize > 100) batchSize = 100; // 因为 restore archive 操作一次只能 100 个 entry
    }

    @Override
    public ChangeType clone() throws CloneNotSupportedException {
        ChangeType changeType = (ChangeType)super.clone();
        changeType.auth = Auth.create(accessId, secretKey);
        changeType.bucketManager = new BucketManager(changeType.auth, configuration);
        changeType.batchOperations = new BatchOperations();
        changeType.lines = new ArrayList<>();
        if (restoreArchive != null) {
            changeType.restoreArchive = new RestoreArchive(changeType.auth, secretKey, configuration, bucket, days, condition);
            changeType.caller = restoreArchive::batchResult;
        }
        return changeType;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    protected List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) throws IOException {
        batchOperations.clearOps();
        lines.clear();
        String key;
        caller.call(processList);
        for (Map<String, String> map : processList) {
            key = map.get("key");
            if (key != null) {
                lines.add(map);
                batchOperations.addChangeTypeOps(bucket, storageType, key);
            } else {
                fileSaveMapper.writeError("key is not exists or empty in " + map, false);
            }
        }
        return lines;
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws IOException {
        return HttpRespUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("key is not exists or empty in " + line);
        if (restoreArchive != null) restoreArchive.singleResult(line);
        Response response = bucketManager.changeType(bucket, key, storageType);
        if (response.statusCode != 200) throw new QiniuException(response);
        response.close();
        return String.join("\t", key, "200");
    }

    @Override
    public void closeResource() {
        super.closeResource();
        batchOperations = null;
        lines = null;
        auth = null;
        configuration = null;
        bucketManager = null;
        caller = null;
    }
}
