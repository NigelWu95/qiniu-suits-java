package com.qiniu.process.qiniu;

import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.CloudApiUtils;
import com.qiniu.util.JsonUtils;

import java.io.IOException;
import java.util.Map;

public class QueryCensorResult extends Base<Map<String, String>> {

    private String jobIdIndex;
    private Configuration configuration;
    private CensorManager censorManager;

    public QueryCensorResult(String accessKey, String secretKey, Configuration configuration, String jobIdIndex) throws IOException {
        super("censorresult", accessKey, secretKey, null);
        this.configuration = configuration;
        if (jobIdIndex == null || "".equals(jobIdIndex)) throw new IOException("please set the id-index.");
        else this.jobIdIndex = jobIdIndex;
        Auth auth = Auth.create(accessKey, secretKey);
        CloudApiUtils.checkQiniu(auth);
        censorManager = new CensorManager(auth, configuration.clone());
    }

    public QueryCensorResult(String accessKey, String secretKey, Configuration configuration, String jobIdIndex, String savePath,
                             int saveIndex) throws IOException {
        super("censorresult", accessKey, secretKey, null, savePath, saveIndex);
        this.configuration = configuration;
        if (jobIdIndex == null || "".equals(jobIdIndex)) throw new IOException("please set the id-index.");
        else this.jobIdIndex = jobIdIndex;
        Auth auth = Auth.create(accessKey, secretKey);
        CloudApiUtils.checkQiniu(auth);
        censorManager = new CensorManager(auth, configuration.clone());
        fileSaveMapper.preAddWriter("waiting");
    }

    public QueryCensorResult(String accessKey, String secretKey, Configuration configuration, String jobIdIndex, String savePath)
            throws IOException {
        this(accessKey, secretKey, configuration, jobIdIndex, savePath, 0);
    }

    @Override
    public QueryCensorResult clone() throws CloneNotSupportedException {
        QueryCensorResult censorResult = (QueryCensorResult)super.clone();
        censorResult.censorManager = new CensorManager(Auth.create(accessId, secretKey), configuration.clone());
        if (censorResult.fileSaveMapper != null) {
            censorResult.fileSaveMapper.preAddWriter("waiting");
        }
        return censorResult;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        StringBuilder ret = new StringBuilder();
        for (String key : line.keySet()) ret.append(line.get(key)).append("\t");
        return ret.deleteCharAt(ret.length() - 1).toString();
    }

    @Override
    protected void parseSingleResult(Map<String, String> line, String result) throws Exception {
        if (result != null && !"".equals(result)) {
            StringBuilder ret = new StringBuilder();
            for (String key : line.keySet()) ret.append(line.get(key)).append("\t");
            CensorResult censorResult = JsonUtils.fromJson(result, CensorResult.class);
            if ("FINISHED".equalsIgnoreCase(censorResult.status)) {
                ret.append(JsonUtils.toJson(censorResult.result));
                fileSaveMapper.writeSuccess(ret.toString(), false);
            } else if ("FAILED".equalsIgnoreCase(censorResult.status)) {
                ret.append(result);
                fileSaveMapper.writeError(ret.toString(), false);
            } else {
                fileSaveMapper.writeToKey("waiting", ret.deleteCharAt(ret.length() - 1).toString(), false);
            }
        } else {
            throw new IOException("only has empty_result");
        }
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String jobId = line.get(jobIdIndex);
        if (jobId == null || jobId.isEmpty()) throw new IOException("id is not exists or empty in " + line);
        return censorManager.censorString(jobId);
    }

    @Override
    public void closeResource() {
        super.closeResource();
        jobIdIndex = null;
        configuration = null;
        censorManager = null;
    }
}
