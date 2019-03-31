package com.qiniu.process.qdora;

import com.google.gson.JsonParseException;
import com.qiniu.common.QiniuException;
import com.qiniu.model.qdora.Item;
import com.qiniu.model.qdora.PfopResult;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryPfopResult extends Base {

    private String pidIndex;
    private MediaManager mediaManager;

    public QueryPfopResult(Configuration configuration, String pidIndex, String savePath, int saveIndex) throws IOException {
        super("pfopresult", "", "", configuration, null, null, savePath, saveIndex);
        set(pidIndex);
        this.mediaManager = new MediaManager(configuration.clone());
    }

    public void updateQuery(String pidIndex) throws IOException {
        set(pidIndex);
    }

    private void set(String pidIndex) throws IOException {
        if (pidIndex == null || "".equals(pidIndex)) throw new IOException("please set the persistentIdIndex.");
        else this.pidIndex = pidIndex;
    }

    public QueryPfopResult(Configuration configuration, String persistentIdIndex, String savePath) throws IOException {
        this(configuration, persistentIdIndex, savePath, 0);
    }

    public QueryPfopResult clone() throws CloneNotSupportedException {
        QueryPfopResult pfopResult = (QueryPfopResult)super.clone();
        pfopResult.mediaManager = new MediaManager(configuration.clone());
        return pfopResult;
    }

    @Override
    protected Map<String, String> formatLine(Map<String, String> line) {
        return line;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get(pidIndex);
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        String result = mediaManager.getPfopResultBodyById(line.get(pidIndex));
        if (result != null && !"".equals(result)) {
            PfopResult pfopResult;
            try {
                pfopResult = JsonConvertUtils.fromJson(result, PfopResult.class);
            } catch (JsonParseException e) {
                throw new QiniuException(e);
            }
            List<String> items = new ArrayList<>();
            // 可能有多条转码指令
            for (Item item : pfopResult.items) {
                // code == 0 时表示转码已经成功，不成功的情况下记录下转码参数和错误方便进行重试
                items.add(line.get(pidIndex) + item.code + "\t" + pfopResult.inputKey + "\t" + item.key + "\t" +
                        item.cmd + "\t" + "\t" + item.desc + "\t" + item.error);
            }
            return String.join("\n", items);
        } else {
            throw new QiniuException(null, "empty_result");
        }
    }
}
