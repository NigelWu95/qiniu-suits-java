package com.qiniu.process.qiniu;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.qiniu.config.JsonFile;
import com.qiniu.model.qdora.Avinfo;
import com.qiniu.model.qdora.VideoStream;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.JsonUtils;
import com.qiniu.util.PfopUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PfopCommand extends Base<Map<String, String>> {

    private boolean hasDuration;
    private boolean hasSize;
    private String avinfoIndex;
    private List<JsonObject> pfopConfigs;
    private boolean combine;
    private Configuration configuration;
    private MediaManager mediaManager;

    public PfopCommand(Configuration configuration, String avinfoIndex, boolean hasDuration, boolean hasSize, boolean combine,
                       String pfopJsonPath, List<JsonObject> pfopConfigs) throws IOException {
        super("pfopcmd", "", "", null);
        set(configuration, avinfoIndex, hasDuration, hasSize, combine, pfopJsonPath, pfopConfigs);
        this.mediaManager = new MediaManager(configuration);
    }

    public PfopCommand(Configuration configuration, String avinfoIndex, boolean hasDuration, boolean hasSize, boolean combine,
                       String pfopJsonPath, List<JsonObject> pfopConfigs, String savePath, int saveIndex) throws IOException {
        super("pfopcmd", "", "", null, savePath, saveIndex);
        set(configuration, avinfoIndex, hasDuration, hasSize, combine, pfopJsonPath, pfopConfigs);
        this.mediaManager = new MediaManager(configuration);
    }

    public PfopCommand(Configuration configuration, String avinfoIndex, boolean hasDuration, boolean hasSize, boolean combine,
                       String pfopJsonPath, List<JsonObject> pfopConfigs, String savePath) throws IOException {
        this(configuration, avinfoIndex, hasDuration, hasSize, combine, pfopJsonPath, pfopConfigs, savePath, 0);
    }

    private void set(Configuration configuration, String avinfoIndex, boolean hasDuration, boolean hasSize, boolean combine,
                     String pfopJsonPath, List<JsonObject> pfopConfigs) throws IOException {
        this.configuration = configuration;
        if (avinfoIndex == null || "".equals(avinfoIndex)) throw new IOException("please set the avinfo-index.");
        else this.avinfoIndex = avinfoIndex;
        this.hasDuration = hasDuration;
        this.hasSize = hasSize;
        if (pfopConfigs != null && pfopConfigs.size() > 0) {
            this.pfopConfigs = pfopConfigs;
        } else if (pfopJsonPath != null && !"".equals(pfopJsonPath)) {
            this.pfopConfigs = new ArrayList<>();
            JsonFile jsonFile = new JsonFile(pfopJsonPath);
            JsonArray array = jsonFile.getElement("pfopcmd").getAsJsonArray();
            for (JsonElement jsonElement : array) {
                JsonObject jsonObject = PfopUtils.checkPfopJson(jsonElement.getAsJsonObject(), false);
                this.pfopConfigs.add(jsonObject);
            }
        } else {
            throw new IOException("please set valid pfop-configs.");
        }
        this.combine = combine;
    }

    @Override
    public PfopCommand clone() throws CloneNotSupportedException {
        PfopCommand pfopCommand = (PfopCommand)super.clone();
        pfopCommand.mediaManager = new MediaManager(configuration);
        return pfopCommand;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return String.join("\t", line.get("key"), line.get(avinfoIndex));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String key = null;
        String info;
        Avinfo avinfo;
        String fopCmd;
        StringBuilder fops = new StringBuilder();
        VideoStream videoStream;
        List<Integer> scale;
        List<String> resultList = new ArrayList<>();
        for (JsonObject pfopConfig : pfopConfigs) {
            key = line.get("key");
            if (key == null) throw new IOException("key is not exists or empty in " + line);
            info = line.get(avinfoIndex);
            if (info == null || "".equals(info)) throw new IOException("avinfo is empty.");
            avinfo = mediaManager.getAvinfoByJson(JsonUtils.fromJson(info, JsonObject.class));
            videoStream = avinfo.getVideoStream();
            if (videoStream == null) throw new IOException("videoStream is null.");
            if (pfopConfig.get("scale") instanceof JsonArray) {
                scale = JsonUtils.fromJsonArray(pfopConfig.get("scale").getAsJsonArray(), new TypeToken<List<Integer>>() {
                });
                if (scale.get(0) < videoStream.width && videoStream.width <= scale.get(1)) {
                    fopCmd = PfopUtils.generateFopCmd(key, pfopConfig);
                } else {
                    continue;
                }
            } else {
                fopCmd = PfopUtils.generateFopCmd(key, pfopConfig);
            }
            if (fopCmd.contains("$(duration)")) fopCmd = fopCmd.replace("$(duration)", avinfo.getFormat().duration);
            if (combine) fops.append(fopCmd);
            else fops.append(key).append("\t").append(fopCmd);
            if (hasDuration) fops.append("\t").append(Double.valueOf(avinfo.getFormat().duration));
            if (hasSize) fops.append("\t").append(Long.parseLong(avinfo.getFormat().size));
            resultList.add(fops.toString());
            fops.delete(0, fops.length());
        }
        if (combine) return String.join("\t", key, String.join(";", resultList));
        else return String.join("\n", resultList);
    }

    @Override
    public void closeResource() {
        super.closeResource();
        avinfoIndex = null;
        pfopConfigs = null;
        configuration = null;
        mediaManager = null;
    }
}
