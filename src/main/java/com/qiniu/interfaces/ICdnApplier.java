package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;

public interface ICdnApplier {

    Response apply(String[] urls) throws QiniuException;
}
