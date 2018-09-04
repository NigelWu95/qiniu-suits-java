package com.qiniu.model;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.qiniu.util.CensorResultUtil;

public class CensorResp {

    private String reqId;
    private JsonObject pulpResult;
    private JsonObject terrorResult;
    private JsonObject politicianResult;

    private CensorResp(String reqId, JsonObject pulpResult, JsonObject terrorResult, JsonObject politicianResult) {
        this.reqId = reqId;
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

    public static CensorResp parseCensorResp(String reqId, String bodyString) {
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(bodyString, JsonObject.class);
        JsonObject pulpResult = jsonObject.has("pulp") ? jsonObject.get("pulp").getAsJsonObject() : null;
        JsonObject terrorResult = jsonObject.has("terror") ? jsonObject.get("terror").getAsJsonObject() : null;
        JsonObject politicianResult = jsonObject.has("politician") ? jsonObject.get("politician").getAsJsonObject() : null;
        return new CensorResp(reqId, pulpResult, terrorResult, politicianResult);
    }

    public boolean isPulpSex() {
        return CensorResultUtil.isPulpSexLabels(CensorResultUtil.getLabels(pulpResult));
    }

    public boolean isPulpYellow() {
        return CensorResultUtil.isPulpYellowLabels(CensorResultUtil.getLabels(pulpResult));
    }

    public boolean isTerror() {
        return CensorResultUtil.isTerrorLabels(CensorResultUtil.getLabels(terrorResult));
    }

    public boolean isPolitician() {
        return CensorResultUtil.isPoliticianLabels(CensorResultUtil.getLabels(politicianResult));
    }
}