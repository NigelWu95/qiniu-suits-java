package com.qiniu.process.qai;

import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.JsonUtils;

import java.io.IOException;
import java.util.Map;

public class QueryCensorResult extends Base<Map<String, String>> {

    private String jobIdIndex;
    private Configuration configuration;
    private CensorManager censorManager;

    public QueryCensorResult(String accesskey, String secretKey, Configuration configuration, String jobIdIndex) throws IOException {
        super("censorresult", accesskey, secretKey, null);
        this.configuration = configuration;
        if (jobIdIndex == null || "".equals(jobIdIndex)) throw new IOException("please set the jobId-index.");
        else this.jobIdIndex = jobIdIndex;
        censorManager = new CensorManager(Auth.create(accesskey, secretKey), configuration.clone());
    }

    public QueryCensorResult(String accesskey, String secretKey, Configuration configuration, String jobIdIndex, String savePath,
                             int saveIndex) throws IOException {
        super("pfopresult", accesskey, secretKey, null, savePath, saveIndex);
        this.configuration = configuration;
        if (jobIdIndex == null || "".equals(jobIdIndex)) throw new IOException("please set the jobId-index.");
        else this.jobIdIndex = jobIdIndex;
        censorManager = new CensorManager(Auth.create(accesskey, secretKey), configuration.clone());
        fileSaveMapper.preAddWriter("waiting");
    }

    public QueryCensorResult(String accesskey, String secretKey, Configuration configuration, String jobIdIndex, String savePath)
            throws IOException {
        this(accesskey, secretKey, configuration, jobIdIndex, savePath, 0);
    }

    public void updateSavePath(String savePath) throws IOException {
        super.updateSavePath(savePath);
        if (fileSaveMapper != null) {
            fileSaveMapper.preAddWriter("waiting");
        }
    }

    public void updateJobIdIndex(String jobIdIndex) {
        this.jobIdIndex = jobIdIndex;
    }

    public QueryCensorResult clone() throws CloneNotSupportedException {
        QueryCensorResult censorResult = (QueryCensorResult)super.clone();
        censorResult.censorManager = new CensorManager(Auth.create(authKey1, authKey2), configuration.clone());
        if (censorResult.fileSaveMapper != null) {
            censorResult.fileSaveMapper.preAddWriter("waiting");
        }
        return censorResult;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get(jobIdIndex);
    }

    @Override
    protected void parseSingleResult(Map<String, String> line, String result) throws Exception {
        if (result != null && !"".equals(result)) {
            CensorResult censorResult = JsonUtils.fromJson(result, CensorResult.class);
            if ("FINISHED".equalsIgnoreCase(censorResult.status))
                fileSaveMapper.writeSuccess(line.get(jobIdIndex) + JsonUtils.toJson(censorResult.result), false);
            else if ("WAITING".equalsIgnoreCase(censorResult.status))
                fileSaveMapper.writeToKey("waiting", line.get(jobIdIndex), false);
            else if ("DOING".equalsIgnoreCase(censorResult.status))
                fileSaveMapper.writeToKey("waiting", line.get(jobIdIndex), false);
            else if ("RESCHEDULED".equalsIgnoreCase(censorResult.status))
                fileSaveMapper.writeToKey("waiting", line.get(jobIdIndex), false);
            else
                fileSaveMapper.writeError(line.get(jobIdIndex), false);
        } else {
            throw new IOException(line + " only has empty_result");
        }
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        String jobId = line.get(jobIdIndex);
        return jobId + "\t" + censorManager.censorString(jobId);
    }

    @Override
    public void closeResource() {
        super.closeResource();
        jobIdIndex = null;
        configuration = null;
        censorManager = null;
    }
}
