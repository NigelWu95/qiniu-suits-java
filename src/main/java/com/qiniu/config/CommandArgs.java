package com.qiniu.config;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CommandArgs implements IEntryParam {

    private String[] params;
    private Map<String, String> paramsMap;

    public CommandArgs(String[] args) throws IOException {
        if (args == null || args.length == 0)
            throw new IOException("args is null");
        else {
            int cmdCount = 0;
            boolean cmdGoon = true;
            paramsMap = new HashMap<>();
            for (String arg : args) {
                // "-" 开头的参数之前放置到 params 数组中
                if (!arg.startsWith("-") && cmdGoon) cmdCount++;
                else {
                    paramsMap.put(splitParam(arg)[0], splitParam(arg)[1]);
                    cmdGoon = false;
                }
            }
            params = new String[cmdCount];
            System.arraycopy(args, 0, params, 0, cmdCount);
        }
    }

    private static String[] splitParam(String paramCommand) throws IOException {

        if (!paramCommand.contains("=") || !paramCommand.startsWith("-")) {
            throw new IOException("there is invalid command param: " + paramCommand + ".");
        }

        String[] strings = paramCommand.substring(1).split("=");
        if (strings.length == 1) {
            throw new IOException("the " + strings[0] + " param has no value.");
        }

        if (strings[1].matches("(\".*\"|\'.*\')"))
            return new String[]{strings[0], strings[1].substring(1, strings[1].length() -1)};
        return strings;
    }

    public String getParamValue(String key) throws IOException {

        if (paramsMap == null) {
            throw new IOException("please set the args.");
        }

        if (!paramsMap.keySet().contains(key)) {
            throw new IOException("not set " + key + " param.");
        }

        return paramsMap.get(key);
    }

    public String[] getParams() {
        return params;
    }
}
