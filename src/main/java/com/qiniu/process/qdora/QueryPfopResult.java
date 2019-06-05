package com.qiniu.process.qdora;

import com.google.gson.JsonParseException;
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
    public String resultInfo(Map<String, String> line) {
        return line.get(pidIndex);
    }

    @Override
    public boolean validCheck(Map<String, String> line) {
        return line.get("key") != null;
    }

    @Override
    public void parseSingleResult(Map<String, String> line, String result) throws IOException {
        if (result != null && !"".equals(result)) {
            PfopResult pfopResult;
            try {
                pfopResult = JsonUtils.fromJson(result, PfopResult.class);
            } catch (JsonParseException e) {
                throw new QiniuException(e, e.getMessage());
            }
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
            throw new QiniuException(null, "empty_result");
        }
    }

    @Override
    public String singleResult(Map<String, String> line) throws IOException {
        return mediaManager.getPfopResultBodyById(line.get(pidIndex));
    }
}
