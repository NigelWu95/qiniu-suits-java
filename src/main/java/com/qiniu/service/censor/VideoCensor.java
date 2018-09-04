package com.qiniu.service.censor;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.model.CensorResp;
import com.qiniu.util.StringMap;

public class VideoCensor {

    private QiniuAuth auth;
    private Client client;
    private Response response;
    private JsonObject dataValueJson;
    private JsonObject paramsValueJson;
    private JsonArray opsValueJsonArray;

    private static volatile VideoCensor videoCensor = null;

    private VideoCensor(QiniuAuth auth) {
        this.auth = auth;
        this.client = new Client();

        this.opsValueJsonArray = new JsonArray();
    }

    public static VideoCensor getVideoCensor(QiniuAuth auth) throws QiniuSuitsException {
        if (videoCensor == null) {
            synchronized (VideoCensor.class) {
                if (videoCensor == null) {
                    videoCensor = new VideoCensor(auth);
                }
            }
        }
        return videoCensor;
    }

    private void setOp(String op, String hookURL, String labelsLabel, int labelsSelect, float labelsScore
            , int terminateMode, String terminateLabel, String terminateLabelMax) {

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

    public CensorResp doCensor(String vid, String url) throws QiniuSuitsException {
        String apiUrl = "http://argus.atlab.ai/v1/video/" + vid;

        JsonObject bodyJson = new JsonObject();
        this.dataValueJson.addProperty("uri", url);
        bodyJson.add("data", dataValueJson);
        bodyJson.add("ops", this.opsValueJsonArray);
        byte[] bodyBytes = bodyJson.toString().getBytes();

        String qiniuToken = "Qiniu " + this.auth.signRequestV2(url, "POST", bodyBytes, "application/json");
        Client client = new Client();
        StringMap headers = new StringMap();
        headers.put("Authorization", qiniuToken);
        String respBody = "";

        try {
            response = client.post(apiUrl, bodyBytes, headers, Client.JsonMime);
        } catch (QiniuException e) {
            QiniuSuitsException qiniuSuitsException = new QiniuSuitsException(e);
            qiniuSuitsException.addToFieldMap("url", url);
            qiniuSuitsException.setStackTrace(e.getStackTrace());
            throw qiniuSuitsException;
        }

        int statusCode = response.statusCode;
        String reqId = response.reqId;

        try {
            respBody = response.bodyString();
        } catch (QiniuException qiniuException) {
            statusCode = 0;
        }

        if (statusCode == 200) {
            return CensorResp.parseCensorResp(reqId, respBody);
        } else {
            QiniuSuitsException qiniuSuitsException = new QiniuSuitsException("video censor error");
            qiniuSuitsException.addToFieldMap("statusCode", String.valueOf(statusCode));
            qiniuSuitsException.addToFieldMap("reqId", reqId);
            qiniuSuitsException.addToFieldMap("respBody", respBody);
            throw qiniuSuitsException;
        }
    }
}