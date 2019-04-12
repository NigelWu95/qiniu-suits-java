package com.qiniu.process.qdora;

import com.google.gson.JsonParseException;
import com.qiniu.common.QiniuException;
import com.qiniu.model.qdora.Item;
import com.qiniu.model.qdora.PfopResult;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.Map;

public class QueryPfopResult extends Base {

    private String protocol;
    private String pidIndex;
    private MediaManager mediaManager;

    public QueryPfopResult(Configuration configuration, String protocol, String persistentIdIndex, String savePath,
                           int saveIndex) throws IOException {
        super("pfopresult", "", "", configuration, null, savePath, saveIndex);
        set(protocol, persistentIdIndex);
        this.mediaManager = new MediaManager(configuration.clone(), protocol);
    }

    public void updateQuery(String protocol, String persistentIdIndex) throws IOException {
        set(protocol, persistentIdIndex);
        this.mediaManager = new MediaManager(configuration.clone(), protocol);
    }

    private void set(String protocol, String pidIndex) throws IOException {
        this.protocol = protocol;
        if (pidIndex == null || "".equals(pidIndex)) throw new IOException("please set the persistentIdIndex.");
        else this.pidIndex = pidIndex;
    }

    public QueryPfopResult(Configuration configuration, String protocol, String persistentIdIndex, String savePath)
            throws IOException {
        this(configuration, protocol, persistentIdIndex, savePath, 0);
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

    // 由于 pfopResult 操作的结果记录方式不同，直接在 singleResult 方法中进行记录，将 base 类的 parseSingleResult 方法重写为空
    @Override
    protected void parseSingleResult(Map<String, String> line, String result) throws IOException {}

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String result = mediaManager.getPfopResultBodyById(line.get(pidIndex));
        if (result != null && !"".equals(result)) {
            PfopResult pfopResult;
            try {
                pfopResult = JsonConvertUtils.fromJson(result, PfopResult.class);
            } catch (JsonParseException e) {
                throw new QiniuException(e, e.getMessage());
            }
            // 可能有多条转码指令
            for (Item item : pfopResult.items) {
                if (item.code == 0)
                    fileMap.writeSuccess(pfopResult.inputKey + "\t" + JsonConvertUtils.toJsonWithoutUrlEscape(item), false);
                else if (item.code == 3)
                    fileMap.writeError(pfopResult.inputKey + "\t" + item.cmd + "\t" +
                            JsonConvertUtils.toJsonWithoutUrlEscape(item), false);
                else if (item.code == 4)
                    fileMap.writeKeyFile("waiting", item.code + "\t" + line.get(pidIndex) + "\t" +
                            JsonConvertUtils.toJsonWithoutUrlEscape(item), false);
                else
                    fileMap.writeKeyFile("notify_failed", item.code + "\t" + line.get(pidIndex) + "\t" +
                            JsonConvertUtils.toJsonWithoutUrlEscape(item), false);
            }
            return null;
        } else {
            throw new QiniuException(null, "empty_result");
        }
    }
}
