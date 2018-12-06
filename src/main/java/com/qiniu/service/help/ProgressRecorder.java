package com.qiniu.service.help;

import com.google.gson.JsonObject;
import com.qiniu.persistence.FileMap;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;

public class ProgressRecorder implements Cloneable {

    private String processName;
    private String resultFileDir;
    private FileMap fileMap;
    private String[] keys;

    public ProgressRecorder(String resultFileDir, String processName, String[] keys) {
        this.processName = processName;
        this.resultFileDir = resultFileDir;
        this.fileMap = new FileMap();
    }

    private void setKeys(String[] keys) {
        this.keys = keys;
    }

    public ProgressRecorder getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        ProgressRecorder progressRecorder = (ProgressRecorder)super.clone();
        progressRecorder.fileMap = new FileMap();
        try {
            progressRecorder.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return progressRecorder;
    }

    public void record(String... progress) {
        JsonObject jsonObject = new JsonObject();
        for (int i = 0; i < keys.length; i++) {
            if (progress.length < i) break;
            jsonObject.addProperty(keys[i], progress[i]);
        }
        fileMap.writeKeyFile("marker" + fileMap.getSuffix(), JsonConvertUtils.toJsonWithoutUrlEscape(jsonObject));
    }
}
