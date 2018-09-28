package com.qiniu.interfaces;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;

public interface IUrlItemProcess {

    QiniuException qiniuException();

    void processItem(String source, String item);

    void processItem(String source, String item, String key);

    void processItem(QiniuAuth auth, String source, String item);

    void processItem(QiniuAuth auth, String source, String item, String key);

    void processUrl(String url, String key);

    void processUrl(String url, String key, String format);

    void processUrl(QiniuAuth auth, String url, String key);

    void processUrl(QiniuAuth auth, String url, String key, String format);

    void closeResource();
}