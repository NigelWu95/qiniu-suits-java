package com.qiniu.service.interfaces;

import com.qiniu.storage.model.FileInfo;

public interface IStringFormat {

    String toFormatString(FileInfo fileInfo);
}