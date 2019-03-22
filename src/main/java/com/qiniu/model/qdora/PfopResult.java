package com.qiniu.model.qdora;

import java.util.ArrayList;
import java.util.List;

public class PfopResult {

    public Integer code;
    public String desc;
    public String id;
    public String inputBucket;
    public String inputKey;
    public List<Item> items = new ArrayList<Item>();
    public String pipeline;
    public String reqid;
}
