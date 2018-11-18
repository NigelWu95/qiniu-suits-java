package com.qiniu.model;

import java.util.ArrayList;
import java.util.List;

public class FetchBody {

    public List<FetchFile> fetchFiles = new ArrayList<>();

    public String host;

    public String bucket;

    public String callbackUrl;

    public String callbackBody;

    public String callbackBodyType;

    public String callbackHost;

    public int fileType;

    public boolean ignoreSameKey;

    public boolean hasCustomArgs() {
        return (host != null || callbackUrl != null || callbackBody != null || callbackBodyType != null
                || callbackHost != null || fileType == 1 || ignoreSameKey);
    }
}
