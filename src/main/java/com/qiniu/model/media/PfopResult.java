package com.qiniu.model.media;

import java.util.ArrayList;
import java.util.List;

public class PfopResult {

    public Integer code;
    public String desc;
    public String id;
    public String inputBucket;
    public String inputKey;
    public List<PfopItem> pfopItems = new ArrayList<PfopItem>();
    public String pipeline;
    public String reqid;
}
