package com.qiniu.service.interfaces;

import java.io.IOException;
import java.util.Map;

public interface ILineParser {

    Map<String, String> getItemMap(String line) throws IOException;
}
