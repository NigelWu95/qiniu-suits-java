package com.qiniu.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public final class CensorResultUtil {

    public static JsonArray getLabels(JsonObject opJson) {
        JsonArray labels = opJson.get("labels").getAsJsonArray();
        return labels;
    }

    public static JsonArray getSegments(JsonObject opJson) {
        JsonArray segments = opJson.get("segments").getAsJsonArray();
        return segments;
    }

    public static boolean isPulpSex(JsonObject labelJson) {
        return "1".equals(labelJson.get("label").getAsString());
    }

    public static boolean isPulpYellow(JsonObject labelJson) {
        return "0".equals(labelJson.get("label").getAsString());
    }

    public static boolean isTerror(JsonObject labelJson) {
        return "1".equals(labelJson.get("label").getAsString());
    }

    public static boolean isPulpSex(JsonObject labelJson, float scoreCriterion) {
        return "1".equals(labelJson.get("label").getAsString()) && labelJson.get("score").getAsFloat() >= scoreCriterion;
    }

    public static boolean isPulpYellow(JsonObject labelJson, float scoreCriterion) {
        return "0".equals(labelJson.get("label").getAsString()) && labelJson.get("score").getAsFloat() >= scoreCriterion;
    }

    public static boolean isTerror(JsonObject labelJson, float scoreCriterion) {
        return "1".equals(labelJson.get("label").getAsString()) && labelJson.get("score").getAsFloat() >= scoreCriterion;
    }

    public static boolean isPulpSexLabels(JsonArray labels) {
        boolean truePulp = false;

        for (JsonElement label : labels) {
            truePulp = isPulpSex(label.getAsJsonObject());

            if (truePulp) {
                break;
            }
        }

        return truePulp;
    }

    public static boolean isPulpYellowLabels(JsonArray labels) {
        boolean truePulp = false;

        for (JsonElement label : labels) {
            truePulp = isPulpYellow(label.getAsJsonObject());

            if (truePulp) {
                break;
            }
        }

        return truePulp;
    }

    public static boolean isTerrorLabels(JsonArray labels) {
        boolean trueTerror = false;

        for (JsonElement label : labels) {
            trueTerror =  isTerror(label.getAsJsonObject());

            if (trueTerror) {
                break;
            }
        }

        return trueTerror;
    }

    public static boolean isPulpSexLabels(JsonArray labels, float scoreCriterion) {
        boolean truePulp = false;

        for (JsonElement label : labels) {
            truePulp = isPulpSex(label.getAsJsonObject(), scoreCriterion);

            if (truePulp) {
                break;
            }
        }

        return truePulp;
    }

    public static boolean isPulpYellowLabels(JsonArray labels, float scoreCriterion) {
        boolean truePulp = false;

        for (JsonElement label : labels) {
            truePulp = isPulpYellow(label.getAsJsonObject(), scoreCriterion);

            if (truePulp) {
                break;
            }
        }

        return truePulp;
    }

    public static boolean isTerrorLabels(JsonArray labels, float scoreCriterion) {
        boolean trueTerror = false;

        for (JsonElement label : labels) {
            trueTerror =  isTerror(label.getAsJsonObject(), scoreCriterion);

            if (trueTerror) {
                break;
            }
        }

        return trueTerror;
    }

    public static boolean isPoliticianLabels(JsonArray labels) {
        return labels.size() > 0;
    }

    public static boolean isPoliticianLabels(JsonArray labels, float scoreCriterion) {
        boolean truePolitician = false;

        if (labels.size() > 0) {
            for (JsonElement label : labels) {
                truePolitician = label.getAsJsonObject().get("score").getAsFloat() >= scoreCriterion;

                if (truePolitician) {
                    break;
                }
            }
        }

        return truePolitician;
    }
}