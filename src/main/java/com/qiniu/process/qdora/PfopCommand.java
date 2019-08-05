package com.qiniu.process.qdora;

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
    private Configuration configuration;
    private MediaManager mediaManager;

    public PfopCommand(Configuration configuration, String avinfoIndex, boolean hasDuration, boolean hasSize,
                       String pfopJsonPath, List<JsonObject> pfopConfigs) throws IOException {
        super("pfopcmd", "", "", null);
        set(configuration, avinfoIndex, hasDuration, hasSize, pfopJsonPath, pfopConfigs);
        this.mediaManager = new MediaManager(configuration.clone());
    }

    public PfopCommand(Configuration configuration, String avinfoIndex, boolean hasDuration, boolean hasSize,
                       String pfopJsonPath, List<JsonObject> pfopConfigs, String savePath, int saveIndex) throws IOException {
        super("pfopcmd", "", "", null, savePath, saveIndex);
        set(configuration, avinfoIndex, hasDuration, hasSize, pfopJsonPath, pfopConfigs);
        this.mediaManager = new MediaManager(configuration.clone());
    }

    public PfopCommand(Configuration configuration, String avinfoIndex, boolean hasDuration, boolean hasSize,
                       String pfopJsonPath, List<JsonObject> pfopConfigs, String savePath) throws IOException {
        this(configuration, avinfoIndex, hasDuration, hasSize, pfopJsonPath, pfopConfigs, savePath, 0);
    }

    private void set(Configuration configuration, String avinfoIndex, boolean hasDuration, boolean hasSize, String pfopJsonPath,
                     List<JsonObject> pfopConfigs) throws IOException {
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
                JsonObject jsonObject = PfopUtils.checkPfopJson(jsonElement.getAsJsonObject(), true);
                this.pfopConfigs.add(jsonObject);
            }
        } else {
            throw new IOException("please set valid pfop-configs.");
        }
    }

    public void updateHasDuration(boolean hasDuration) {
        this.hasDuration = hasDuration;
    }

    public void updateHasSize(boolean hasSize) {
        this.hasSize = hasSize;
    }

    public void updateAvinfoIndex(String avinfoIndex) {
        this.avinfoIndex = avinfoIndex;
    }

    public void updateFopsConfig(String pfopJsonPath, List<JsonObject> pfopConfigs) throws IOException {
        if (pfopConfigs != null && pfopConfigs.size() > 0) {
            this.pfopConfigs = pfopConfigs;
        } else if (pfopJsonPath != null && !"".equals(pfopJsonPath)) {
            this.pfopConfigs = new ArrayList<>();
            JsonFile jsonFile = new JsonFile(pfopJsonPath);
            JsonArray array = jsonFile.getElement("pfop").getAsJsonArray();
            for (JsonElement jsonElement : array) {
                JsonObject jsonObject = PfopUtils.checkPfopJson(jsonElement.getAsJsonObject(), false);
                this.pfopConfigs.add(jsonObject);
            }
        } else {
            throw new IOException("please set valid pfop-configs.");
        }
    }

    @SuppressWarnings("unchecked")
    public PfopCommand clone() throws CloneNotSupportedException {
        PfopCommand pfopCommand = (PfopCommand)super.clone();
        pfopCommand.mediaManager = new MediaManager(configuration.clone());
        return pfopCommand;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key") + "\t" + line.get(avinfoIndex);
    }

    @Override
    protected String singleResult(Map<String, String> line) throws Exception {
        String key;
        String info;
        Avinfo avinfo;
        StringBuilder other = new StringBuilder();
        VideoStream videoStream;
        List<Integer> scale;
        List<String> resultList = new ArrayList<>();
        for (JsonObject pfopConfig : pfopConfigs) {
            scale = JsonUtils.fromJsonArray(pfopConfig.get("scale").getAsJsonArray(), new TypeToken<List<Integer>>(){});
            key = line.get("key");
            if (key == null) throw new IOException("no key in " + line);
            info = line.get(avinfoIndex);
            if (info == null || "".equals(info)) throw new IOException("avinfo is empty.");
            avinfo = mediaManager.getAvinfoByJson(JsonUtils.fromJson(info, JsonObject.class));
            if (hasDuration) other.append("\t").append(Double.valueOf(avinfo.getFormat().duration));
            if (hasSize) other.append("\t").append(Long.valueOf(avinfo.getFormat().size));
            videoStream = avinfo.getVideoStream();
            if (videoStream == null) throw new IOException("videoStream is null.");
            if (scale.get(0) < videoStream.width && videoStream.width <= scale.get(1)) {
                resultList.add(key + "\t" + PfopUtils.generateFopCmd(key, pfopConfig) + other.toString());
            }
        }
        return String.join("\n", resultList);
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
