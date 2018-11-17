package com.qiniu.interfaces;

import com.qiniu.common.QiniuException;
import com.qiniu.util.Auth;

public interface IUrlItemProcess {

    QiniuException qiniuException();

    String getProcessName();

    void processItem(String source, String item);

    void processItem(String source, String item, String key);

    void processItem(Auth auth, String source, String item);

    void processItem(Auth auth, String source, String item, String key);

    void processUrl(String url, String key);

    void processUrl(String url, String key, String format);

    void processUrl(Auth auth, String url, String key);

    void processUrl(Auth auth, String url, String key, String format);

    void closeResource();
}
