package com.qiniu.service.jedi;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.FileReaderAndWriterMap;
import com.qiniu.config.PropertyConfig;
import com.qiniu.interfaces.IUrlItemProcess;
import com.qiniu.util.DateUtil;

import java.util.Map;

public class VideoExport {

    private QiniuAuth jediAccountAuth;
    private String jediSource;
    private String pointTime;
    private boolean pointTimeIsBiggerThanTimeStamp;

    public VideoExport() {
        PropertyConfig propertyConfig = new PropertyConfig(".qiniu.properties");
        this.jediAccountAuth = QiniuAuth.create(propertyConfig.getProperty("jedi_access_key"), propertyConfig.getProperty("jedi_secret_key"));
        this.jediSource = propertyConfig.getProperty("jedi_source");
        this.pointTimeIsBiggerThanTimeStamp = true;
    }

    public void setPointTime(String pointTime, boolean pointTimeIsBiggerThanTimeStamp) {
        this.pointTime = pointTime;
        this.pointTimeIsBiggerThanTimeStamp = pointTimeIsBiggerThanTimeStamp;
    }

    public Map<String, Object> getFirstResult(QiniuAuth auth, String jediHub) {
        VideoManage vm = new VideoManage(auth);
        Map<String, Object> result = vm.getVideoInfoList(jediHub, null, 1);
        return result;
    }

    public long getTotalCount(QiniuAuth auth, String jediHub) {
        Gson gson = new Gson();
        long total = gson.fromJson((String) getFirstResult(auth, jediHub).get("msg"), JsonObject.class).get("total").getAsInt();
        return total;
    }

    public JsonArray getFirstItems(QiniuAuth auth, String jediHub) {
        Gson gson = new Gson();
        JsonArray jsonElements = gson.fromJson((String) getFirstResult(auth, jediHub).get("msg"), JsonObject.class).getAsJsonArray("items");
        return jsonElements;
    }

    public void processUrlGroupbyFormat(FileReaderAndWriterMap targetFileReaderAndWriterMap, JsonArray jsonElements, IUrlItemProcess processor) {
        JsonObject item = null;
        JsonArray transcoding = null;
        JsonObject transcodingResult = null;
        Exporter exporter = new Exporter();
        boolean isDoProcess = false;

        for (int i = 0; i < jsonElements.size(); i++) {
            item = jsonElements.get(i).getAsJsonObject();

            try {
                transcoding = item.get("transcoding").getAsJsonArray();
            } catch (NullPointerException nullException) {
                targetFileReaderAndWriterMap.writeErrorAndNull("transcoding null:" + item.toString());
                continue;
            }

            exporter.setKey(item.get("key").getAsString());
            exporter.setName(item.get("name").getAsString());
            String modifiedTime = item.get("modification_time").getAsString();

            try {
                // 相较于时间节点的记录进行处理，并保存请求状态码和 id 到文件中。
                isDoProcess = DateUtil.compareTimeToBreakpoint(pointTime, pointTimeIsBiggerThanTimeStamp, modifiedTime);
            } catch (Exception ex) {
                targetFileReaderAndWriterMap.writeErrorAndNull("date error:" + item.toString());
                continue;
            }

            if (isDoProcess) {
                // 原文件名进行处理，该文件在点播的 src 空间中，要单独设置源来获取
                processor.processItem(this.jediAccountAuth, this.jediSource, exporter.getKey());

                for (int formatIndex = 0; formatIndex < transcoding.size(); formatIndex++) {
                    transcodingResult = transcoding.get(formatIndex).getAsJsonObject();
                    exporter.setFormat(transcodingResult.get("format") == null ? "" : transcodingResult.get("format").getAsString());
                    exporter.setStatus(transcodingResult.get("status") == null ? "" : transcodingResult.get("status").getAsString());
                    exporter.setUrl(transcodingResult.get("url") == null ? "" : transcodingResult.get("url").getAsString());

                    if ("succeed".equals(exporter.getStatus()) && !"".equals(exporter.getUrl())) {
                        targetFileReaderAndWriterMap.writeSuccess(exporter.toString());
                        targetFileReaderAndWriterMap.writeKeyFile(exporter.getFormat(), exporter.getUrl());

                        // url 按照文件格式进行处理, 带上处理之后的文件名
                        processor.processUrl(exporter.getUrl(), exporter.getUrl().split("(https?://(\\S+\\.){1,5}\\S+/)|(\\?ver=)")[1], exporter.getFormat());
                    } else {
                        targetFileReaderAndWriterMap.writeErrorAndNull(exporter.toString());
                    }
                }
            }
        }
    }
}