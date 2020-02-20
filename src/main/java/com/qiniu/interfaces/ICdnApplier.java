package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;

import java.util.List;

public interface ICdnApplier {

    Response apply(List<String> urls) throws QiniuException;
}
