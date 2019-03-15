package com.qiniu.service.media;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.qiniu.config.JsonFile;
import com.qiniu.model.media.Avinfo;
import com.qiniu.model.media.VideoStream;
import com.qiniu.persistence.FileMap;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.FileNameUtils;
import com.qiniu.util.JsonConvertUtils;
import com.qiniu.util.UrlSafeBase64;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PfopCommand implements ILineProcess<Map<String, String>>, Cloneable {

    final private String processName;
    private MediaManager mediaManager;
    private FileMap fileMap;
    private boolean hasDuration;
    private boolean hasSize;
    private String avinfoIndex;
    private ArrayList<JsonObject> pfopConfigs;
    final private String rmPrefix;
    private String savePath;
    private String saveTag;
    private int saveIndex;

    public PfopCommand(String jsonPath, boolean hasDuration, boolean hasSize, String avinfoIndex, String rmPrefix,
                       String savePath, int saveIndex) throws IOException {
        this.processName = "pfopcmd";
        pfopConfigs = new ArrayList<>();
        JsonFile jsonFile = new JsonFile(jsonPath);
        for (String key : jsonFile.getConfigKeys()) {
            JsonObject jsonObject = jsonFile.getElement(key).getAsJsonObject();
            List<Integer> scale = JsonConvertUtils.fromJsonArray(jsonObject.get("scale").getAsJsonArray(),
                    new TypeToken<List<Integer>>(){});
            if (scale.size() < 1) {
                throw new IOException(jsonPath + " miss the scale field in \"" + key + "\"");
            } else if (scale.size() == 1) {
                JsonArray jsonArray = new JsonArray();
                jsonArray.add(scale.get(0));
                jsonArray.add(Integer.MAX_VALUE);
                jsonObject.add("scale", jsonArray);
            }
            if (!jsonObject.keySet().contains("cmd") || !jsonObject.keySet().contains("saveas"))
                throw new IOException(jsonPath + " miss the \"cmd\" or \"saveas\" fields in \"" + key + "\"");
            else if (!jsonObject.get("saveas").getAsString().contains(":"))
                throw new IOException(jsonPath + " miss the <bucket> field of \"saveas\" field in \"" + key + "\"");
            jsonObject.addProperty("name", key);
            pfopConfigs.add(jsonObject);
        }
        this.mediaManager = new MediaManager();
        this.hasDuration = hasDuration;
        this.hasSize = hasSize;
        if (avinfoIndex == null || "".equals(avinfoIndex)) throw new IOException("please set the avinfoIndex.");
        else this.avinfoIndex = avinfoIndex;
        this.rmPrefix = rmPrefix;
        this.savePath = savePath;
        this.saveTag = "";
        this.saveIndex = saveIndex;
        this.fileMap = new FileMap(savePath, processName, String.valueOf(saveIndex));
        this.fileMap.initDefaultWriters();
    }

    public PfopCommand(String jsonPath, boolean hasDuration, boolean hasSize, String avinfoIndex, String rmPrefix,
                       String savePath) throws IOException {
        this(jsonPath, hasDuration, hasSize, avinfoIndex, rmPrefix, savePath, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setSaveTag(String saveTag) {
        this.saveTag = saveTag == null ? "" : saveTag;
    }

    @SuppressWarnings("unchecked")
    public PfopCommand clone() throws CloneNotSupportedException {
        PfopCommand pfopCommand = (PfopCommand)super.clone();
        pfopCommand.mediaManager = new MediaManager();
        pfopCommand.pfopConfigs = (ArrayList<JsonObject>) pfopConfigs.clone();
        pfopCommand.fileMap = new FileMap(savePath, processName, saveTag + String.valueOf(++saveIndex));
        try {
            pfopCommand.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return pfopCommand;
    }

    private String generateFopCmd(String srcKey, JsonObject pfopJson) throws IOException {
        String saveAs = pfopJson.get("saveas").getAsString();
        String saveAsKey = saveAs.substring(saveAs.indexOf(":") + 1);
        if (saveAsKey.contains("$(key)")) {
            if (saveAsKey.contains(".")) {
                String[] nameParts = saveAsKey.split("(\\$\\(key\\)|\\.)");
                saveAsKey = FileNameUtils.addPrefixAndSuffixWithExt(nameParts[0], srcKey, nameParts[1], nameParts[2]);
            } else {
                String[] nameParts = saveAsKey.split("\\$\\(key\\)");
                saveAsKey = FileNameUtils.addPrefixAndSuffixKeepExt(nameParts[0], srcKey, nameParts[1]);
            }
            saveAs = saveAs.replace(saveAs.substring(saveAs.indexOf(":") + 1), saveAsKey);
        }
        return pfopJson.get("cmd").getAsString() + "|saveas/" + UrlSafeBase64.encodeToString(saveAs);
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        String key;
        String info;
        Avinfo avinfo;
        StringBuilder other = new StringBuilder();
        VideoStream videoStream;
        List<Integer> scale;
        for (JsonObject pfopConfig : pfopConfigs) {
            scale = JsonConvertUtils.fromJsonArray(pfopConfig.get("scale").getAsJsonArray(),
                    new TypeToken<List<Integer>>(){});
            for (Map<String, String> line : lineList) {
                key = line.get("key");
                info = line.get(avinfoIndex);
                try {
                    if (key == null || "".equals(key) || info == null || "".equals(info))
                        throw new IOException("key or avinfo is empty.");
                    key = FileNameUtils.rmPrefix(rmPrefix, line.get("key"));
                    avinfo = mediaManager.getAvinfoByJson(info);
                    if (hasDuration) other.append("\t").append(Double.valueOf(avinfo.getFormat().duration));
                    if (hasSize) other.append("\t").append(Long.valueOf(avinfo.getFormat().size));
                    videoStream = avinfo.getVideoStream();
                    if (videoStream == null) throw new Exception("videoStream is null");
                    if (scale.get(0) < videoStream.width && videoStream.width <= scale.get(1)) {
                        fileMap.writeSuccess(key + "\t" + generateFopCmd(key, pfopConfig) + other.toString(), false);
                    }
                } catch (Exception e) {
                    fileMap.writeError(String.valueOf(line) + "\t" + e.getMessage(), false);
                }
            }
        }
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
