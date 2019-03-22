package com.qiniu.interfaces;

import java.io.IOException;
import java.util.Map;

public interface ILineParser<T> {

    Map<String, String> getItemMap(T line) throws IOException;
}
