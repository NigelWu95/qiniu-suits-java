package com.qiniu.service.jedi;

import com.qiniu.common.QiniuAuth;

public interface IProcessInterface {

    void processItem(String rootUrl, String item);

    void processItem(String rootUrl, String item, String key);

    void processItem(QiniuAuth auth, String rootUrl, String item);

    void processItem(QiniuAuth auth, String rootUrl, String item, String key);

    void processUrl(String url, String key);

    void processUrl(String url, String key, String format);

    void processUrl(QiniuAuth auth, String url, String key);

    void processUrl(QiniuAuth auth, String url, String key, String format);

    void close();
}