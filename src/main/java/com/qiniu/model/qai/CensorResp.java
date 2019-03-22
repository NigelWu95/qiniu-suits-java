package com.qiniu.model.qai;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.qiniu.util.CensorResultUtils;

public class CensorResp {

    private JsonObject pulpResult;
    private JsonObject terrorResult;
    private JsonObject politicianResult;

    private CensorResp(JsonObject pulpResult, JsonObject terrorResult, JsonObject politicianResult) {
        this.pulpResult = pulpResult;
        this.terrorResult = terrorResult;
        this.politicianResult = politicianResult;
    }

    public JsonObject getPulpResult() {
        return pulpResult;
    }

    public JsonObject getTerrorResult() {
        return terrorResult;
    }

    public JsonObject getPoliticianResult() {
        return politicianResult;
    }

    public static CensorResp parseCensorResp(String bodyString) {
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(bodyString, JsonObject.class);
        JsonObject pulpResult = jsonObject.has("pulp") ? jsonObject.get("pulp").getAsJsonObject() : null;
        JsonObject terrorResult = jsonObject.has("terror") ? jsonObject.get("terror").getAsJsonObject() : null;
        JsonObject politicianResult = jsonObject.has("politician") ? jsonObject.get("politician").getAsJsonObject() : null;
        return new CensorResp(pulpResult, terrorResult, politicianResult);
    }

    public boolean isPulpSex() {
        return CensorResultUtils.isPulpSexLabels(CensorResultUtils.getLabels(pulpResult));
    }

    public boolean isPulpYellow() {
        return CensorResultUtils.isPulpYellowLabels(CensorResultUtils.getLabels(pulpResult));
    }

    public boolean isTerror() {
        return CensorResultUtils.isTerrorLabels(CensorResultUtils.getLabels(terrorResult));
    }

    public boolean isPolitician() {
        return CensorResultUtils.isPoliticianLabels(CensorResultUtils.getLabels(politicianResult));
    }
}
