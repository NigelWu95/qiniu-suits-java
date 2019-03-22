package com.qiniu.service.qai;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.model.qai.CensorResp;
import com.qiniu.util.Auth;
import com.qiniu.util.StringMap;

public class VideoCensor {

    private Auth auth;
    private Client client;
    private JsonObject dataValueJson;
    private JsonObject paramsValueJson;
    private JsonArray opsValueJsonArray;

    private static volatile VideoCensor videoCensor = null;

    private VideoCensor(Auth auth) {
        this.auth = auth;
        this.client = new Client();
        this.dataValueJson = new JsonObject();
        this.paramsValueJson = new JsonObject();
        this.opsValueJsonArray = new JsonArray();
    }

    public static VideoCensor getInstance(Auth auth) {
        if (videoCensor == null) {
            synchronized (VideoCensor.class) {
                if (videoCensor == null) {
                    videoCensor = new VideoCensor(auth);
                }
            }
        }
        return videoCensor;
    }

    private void setOp(String op, String hookURL, String labelsLabel, int labelsSelect, float labelsScore,
                       int terminateMode, String terminateLabel, String terminateLabelMax) {

        JsonObject innerLabels = new JsonObject();
        innerLabels.addProperty("label", labelsLabel);
        innerLabels.addProperty("select", labelsSelect);
        innerLabels.addProperty("score", labelsScore);
        JsonArray labelsArrays = new JsonArray();
        labelsArrays.add(innerLabels);

        JsonObject terminateLabels = new JsonObject();
        terminateLabels.addProperty(terminateLabel, terminateLabelMax);
        JsonObject innerTerminate = new JsonObject();
        innerTerminate.addProperty("mode", terminateMode);
        innerTerminate.add("labels", terminateLabels);

        JsonObject paramsJson = new JsonObject();
        paramsJson.add("labels", labelsArrays);
        paramsJson.add("terminate", innerTerminate);

        JsonObject opValue = new JsonObject();
        opValue.addProperty("op", op);
        opValue.addProperty("hookURL", hookURL);
        opValue.add("params", paramsJson);

        this.opsValueJsonArray.add(opValue);
    }

    public void setPulpOps(String hookURL, String labelsLabel, int labelsSelect, float labelsScore
            , int terminateMode, String terminateLabel, String terminateLabelMax) {
        setOp("pulp", hookURL, labelsLabel, labelsSelect, labelsScore, terminateMode, terminateLabel, terminateLabelMax);
    }

    public void setTerrorOps(String hookURL, String labelsLabel, int labelsSelect, float labelsScore
            , int terminateMode, String terminateLabel, String terminateLabelMax) {
        setOp("terror", hookURL, labelsLabel, labelsSelect, labelsScore, terminateMode, terminateLabel, terminateLabelMax);
    }

    public void setPoliticianOps(String hookURL, String labelsLabel, int labelsSelect, float labelsScore
            , int terminateMode, String terminateLabel, String terminateLabelMax) {
        setOp("politician", hookURL, labelsLabel, labelsSelect, labelsScore, terminateMode, terminateLabel, terminateLabelMax);
    }

    public void setCensorParams(boolean async, int mode, int interval, String bucket, String prefix, String hookURL) {
        JsonObject vframeSetting = new JsonObject();
        vframeSetting.addProperty("mode", mode);
        vframeSetting.addProperty("interval", interval);
        JsonObject saveSetting = new JsonObject();
        saveSetting.addProperty("bucket", bucket);
        saveSetting.addProperty("prefix", prefix);
        this.paramsValueJson.addProperty("async", async);
        this.paramsValueJson.addProperty("hookURL", hookURL);
        this.paramsValueJson.add("vframe", vframeSetting);
        this.paramsValueJson.add("save", saveSetting);
    }

    public CensorResp doCensor(String vid, String url) throws QiniuException {
        String apiUrl = "http://argus.atlab.ai/v1/video/" + vid;

        JsonObject bodyJson = new JsonObject();
        this.dataValueJson.addProperty("uri", url);
        bodyJson.add("data", dataValueJson);
        bodyJson.add("ops", this.opsValueJsonArray);
        byte[] bodyBytes = bodyJson.toString().getBytes();
        String qiniuToken = "Qiniu " + this.auth.signRequestV2(url, "POST", bodyBytes, "application/json");
        StringMap headers = new StringMap();
        headers.put("Authorization", qiniuToken);
        Response response = client.post(apiUrl, bodyBytes, headers, Client.JsonMime);
        String respBody = response.bodyString();
        return CensorResp.parseCensorResp(respBody);
    }
}
