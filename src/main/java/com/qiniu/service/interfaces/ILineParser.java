package com.qiniu.service.interfaces;

import java.util.ArrayList;
import java.util.Map;

public interface ILineParser {

    ArrayList<String> splitLine(String line);

    Map<String, String> getItemMapByKeys(String line, ArrayList<String> itemKey);

    Map<String, String> getItemMap(String line);
}
