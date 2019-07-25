package com.qiniu.process.qdora;

import com.qiniu.common.QiniuException;
import com.qiniu.model.qdora.Item;
import com.qiniu.model.qdora.PfopResult;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.JsonUtils;

import java.io.IOException;
import java.util.Map;

public class QueryPfopResult extends Base<Map<String, String>> {

    private String protocol;
    private String pidIndex;
    private Configuration configuration;
    private MediaManager mediaManager;

    public QueryPfopResult(Configuration configuration, String protocol, String persistentIdIndex) throws IOException {
        super("pfopresult", "", "", null);
        set(configuration, protocol, persistentIdIndex);
        this.mediaManager = new MediaManager(configuration.clone(), protocol);
    }

    public QueryPfopResult(Configuration configuration, String protocol, String persistentIdIndex, String savePath,
                           int saveIndex) throws IOException {
        super("pfopresult", "", "", null, savePath, saveIndex);
        set(configuration, protocol, persistentIdIndex);
        this.mediaManager = new MediaManager(configuration.clone(), protocol);
    }

    public QueryPfopResult(Configuration configuration, String protocol, String persistentIdIndex, String savePath)
            throws IOException {
        this(configuration, protocol, persistentIdIndex, savePath, 0);
    }

    private void set(Configuration configuration, String protocol, String pidIndex) throws IOException {
        this.configuration = configuration;
        this.protocol = protocol;
        if (pidIndex == null || "".equals(pidIndex)) throw new IOException("please set the persistentId-index.");
        else this.pidIndex = pidIndex;
    }

    public void updateProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void updatePidIndex(String pidIndex) {
        this.pidIndex = pidIndex;
    }

    public QueryPfopResult clone() throws CloneNotSupportedException {
        QueryPfopResult pfopResult = (QueryPfopResult)super.clone();
        pfopResult.mediaManager = new MediaManager(configuration.clone(), protocol);
        return pfopResult;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get(pidIndex);
    }

    @Override
    protected boolean validCheck(Map<String, String> line) {
        return line.get(pidIndex) != null;
    }

    @Override
    protected void parseSingleResult(Map<String, String> line, String result) throws Exception {
        if (result != null && !"".equals(result)) {
            PfopResult pfopResult = JsonUtils.fromJson(result, PfopResult.class);
            // 可能有多条转码指令
            for (Item item : pfopResult.items) {
                if (item.code == 0)
                    fileSaveMapper.writeSuccess(pfopResult.inputKey + "\t" + (item.key != null ? item.key + "\t" : "") +
                            JsonUtils.toJsonWithoutUrlEscape(item), false);
                else if (item.code == 3)
                    fileSaveMapper.writeError(pfopResult.inputKey + "\t" + item.cmd + "\t" +
                            JsonUtils.toJsonWithoutUrlEscape(item), false);
                else if (item.code == 4)
                    fileSaveMapper.writeKeyFile("waiting", item.code + "\t" + line.get(pidIndex) + "\t" +
                            JsonUtils.toJsonWithoutUrlEscape(item), false);
                else
                    fileSaveMapper.writeKeyFile("notify_failed", item.code + "\t" + line.get(pidIndex) + "\t" +
                        JsonUtils.toJsonWithoutUrlEscape(item), false);
            }
        } else {
            throw new IOException(line + " only has empty_result");
        }
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        String pid = line.get(pidIndex);
        return pid + "\t" + mediaManager.getPfopResultBodyById(pid);
    }

    @Override
    public void closeResource() {
        super.closeResource();
        configuration = null;
        mediaManager = null;
        protocol = null;
        pidIndex = null;
    }
}
