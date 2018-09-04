package com.qiniu.service.impl;

import com.qiniu.common.QiniuAuth;
import com.qiniu.interfaces.IUrlItemProcess;

public class NothingUrlItemProcess implements IUrlItemProcess {

    @Override
    public void processItem(String rootUrl, String item) {

    }

    @Override
    public void processItem(String rootUrl, String item, String key) {

    }

    @Override
    public void processItem(QiniuAuth auth, String rootUrl, String item) {

    }

    @Override
    public void processItem(QiniuAuth auth, String rootUrl, String item, String key) {

    }

    @Override
    public void processUrl(String url, String key) {

    }

    @Override
    public void processUrl(String url, String key, String format) {

    }

    @Override
    public void processUrl(QiniuAuth auth, String url, String key) {

    }

    @Override
    public void processUrl(QiniuAuth auth, String url, String key, String format) {

    }

    @Override
    public void close() {

    }
}