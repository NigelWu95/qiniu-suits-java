package com.qiniu.process.qdora;

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

    public QueryPfopResult(Configuration configuration, String protocol, String pIdIndex) throws IOException {
        super("pfopresult", "", "", null);
        set(configuration, protocol, pIdIndex);
        this.mediaManager = new MediaManager(configuration.clone(), protocol);
    }

    public QueryPfopResult(Configuration configuration, String protocol, String pIdIndex, String savePath,
                           int saveIndex) throws IOException {
        super("pfopresult", "", "", null, savePath, saveIndex);
        set(configuration, protocol, pIdIndex);
        this.mediaManager = new MediaManager(configuration.clone(), protocol);
        this.fileSaveMapper.preAddWriter("waiting");
        this.fileSaveMapper.preAddWriter("notify_failed");
    }

    public QueryPfopResult(Configuration configuration, String protocol, String pIdIndex, String savePath)
            throws IOException {
        this(configuration, protocol, pIdIndex, savePath, 0);
    }

    private void set(Configuration configuration, String protocol, String pidIndex) throws IOException {
        this.configuration = configuration;
        this.protocol = protocol == null || !protocol.matches("(http|https)") ? "http" : protocol;
        if (pidIndex == null || "".equals(pidIndex)) throw new IOException("please set the id-index.");
        else this.pidIndex = pidIndex;
    }

    public void updateSavePath(String savePath) throws IOException {
        super.updateSavePath(savePath);
        if (fileSaveMapper != null) {
            this.fileSaveMapper.preAddWriter("waiting");
            this.fileSaveMapper.preAddWriter("notify_failed");
        }
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
        if (pfopResult.fileSaveMapper != null) {
            pfopResult.fileSaveMapper.preAddWriter("waiting");
            pfopResult.fileSaveMapper.preAddWriter("notify_failed");
        }
        return pfopResult;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get(pidIndex);
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
                    fileSaveMapper.writeToKey("waiting", item.code + "\t" + line.get(pidIndex) + "\t" +
                            JsonUtils.toJsonWithoutUrlEscape(item), false);
                else
                    fileSaveMapper.writeToKey("notify_failed", item.code + "\t" + line.get(pidIndex) + "\t" +
                        JsonUtils.toJsonWithoutUrlEscape(item), false);
            }
        } else {
            throw new IOException(line + " only has empty_result");
        }
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String pid = line.get(pidIndex);
        if (pid == null || pid.isEmpty()) throw new IOException("id is not exists or empty in " + line);
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
