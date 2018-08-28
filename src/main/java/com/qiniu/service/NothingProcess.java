package com.qiniu.service;

import com.qiniu.common.QiniuAuth;
import com.qiniu.service.jedi.IProcessInterface;

public class NothingProcess implements IProcessInterface {

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