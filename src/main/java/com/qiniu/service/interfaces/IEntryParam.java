package com.qiniu.service.interfaces;

import java.io.IOException;

public interface IEntryParam {

    String getParamValue(String key) throws IOException;
}
