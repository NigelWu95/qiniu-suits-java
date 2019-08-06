package com.qiniu.process.qai;

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
        if (jobIdIndex == null || "".equals(jobIdIndex)) throw new IOException("please set the id-index.");
        else this.jobIdIndex = jobIdIndex;
        censorManager = new CensorManager(Auth.create(accesskey, secretKey), configuration.clone());
    }

    public QueryCensorResult(String accesskey, String secretKey, Configuration configuration, String jobIdIndex, String savePath,
                             int saveIndex) throws IOException {
        super("censorresult", accesskey, secretKey, null, savePath, saveIndex);
        this.configuration = configuration;
        if (jobIdIndex == null || "".equals(jobIdIndex)) throw new IOException("please set the id-index.");
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
            StringBuilder ret = new StringBuilder();
            for (String key : line.keySet()) ret.append(line.get(key)).append("\t");
            CensorResult censorResult = JsonUtils.fromJson(result, CensorResult.class);
            if ("FINISHED".equalsIgnoreCase(censorResult.status)) {
                ret.append(JsonUtils.toJson(censorResult.result));
                fileSaveMapper.writeSuccess(ret.toString(), false);
            } else if ("WAITING".equalsIgnoreCase(censorResult.status))
                fileSaveMapper.writeToKey("waiting", ret.deleteCharAt(ret.length() - 1).toString(), false);
            else if ("DOING".equalsIgnoreCase(censorResult.status))
                fileSaveMapper.writeToKey("waiting", ret.deleteCharAt(ret.length() - 1).toString(), false);
            else if ("RESCHEDULED".equalsIgnoreCase(censorResult.status))
                fileSaveMapper.writeToKey("waiting", ret.deleteCharAt(ret.length() - 1).toString(), false);
            else
                fileSaveMapper.writeError(line.get(jobIdIndex), false);
        } else {
            throw new IOException(line + " only has empty_result");
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
