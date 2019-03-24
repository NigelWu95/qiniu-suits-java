package com.qiniu.process.qdora;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.qiniu.common.QiniuException;
import com.qiniu.model.qdora.Item;
import com.qiniu.model.qdora.PfopResult;
import com.qiniu.process.Base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryPfopResult extends Base {

    final private String pidIndex;
    private MediaManager mediaManager;
    private Gson gson;

    public QueryPfopResult(String pidIndex, String savePath, int saveIndex) throws IOException {
        super("pfopresult", null, null, null, null, null, savePath, saveIndex);
        if (pidIndex == null || "".equals(pidIndex)) throw new IOException("please set the persistentIdIndex.");
        else this.pidIndex = pidIndex;
        this.mediaManager = new MediaManager();
        this.gson = new Gson();
    }

    public QueryPfopResult(String persistentIdIndex, String savePath) throws IOException {
        this(persistentIdIndex, savePath, 0);
    }

    public QueryPfopResult clone() throws CloneNotSupportedException {
        QueryPfopResult pfopResult = (QueryPfopResult)super.clone();
        pfopResult.mediaManager = new MediaManager();
        pfopResult.gson = new Gson();
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

    protected String singleResult(Map<String, String> line) throws QiniuException {
        String result = mediaManager.getPfopResultBodyById(line.get(pidIndex));
        if (result != null && !"".equals(result)) {
            PfopResult pfopResult;
            try {
                pfopResult = gson.fromJson(result, PfopResult.class);
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
