package com.qiniu.service.FileLine;

import com.qiniu.common.QiniuAuth;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.interfaces.IUrlItemProcess;

public class NothingProcess implements IUrlItemProcess, IOssFileProcess {

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
    public void processFile(String fileInfoStr) {

    }

    @Override
    public void closeResource() {

    }
}