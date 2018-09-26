package com.qiniu.util;

import java.util.HashMap;
import java.util.Map;

public class MainArgsUtils {

    private static String[] params = new String[]{};

    private static Map<String, String> paramsMap = new HashMap<>();

    private static void setParams(String[] args) throws Exception {

        if (args != null) {
            if (params.length == args.length)
                return;
            params = new String[args.length];
            System.arraycopy(args, 0, params, 0, args.length);
        }
        else
            throw new Exception("args is null");
    }

    public static String getParam(String[] args, int index, String error) throws Exception {

        try {
            setParams(args);
        } catch (Exception e) {
            throw new Exception(e.getMessage() + ", " + error);
        }

        if (index < 1) {
            throw new Exception("index must be more than 1.");
        }

        if (args.length < index) {
            throw new Exception("index is out of bound, " + error);
        }

        System.out.println(params[index - 1]);
        return params[index - 1];
    }

    public static String[] splitParam(String paramCommand) throws Exception {

        if (!paramCommand.contains("=") || !paramCommand.startsWith("-")) {
            throw new Exception("it is invalid command param.");
        }

        return paramCommand.substring(1).split("=");
    }

    public static void setParamsMap(String[] args) throws Exception {

        if (args != null) {
            if (paramsMap.size() == args.length)
                return;

            for (String arg : args) {
                paramsMap.put(splitParam(arg)[0], splitParam(arg)[1]);
            }
        }
        else
            throw new Exception("args is null");
    }

    public static String getParamValue(String key) throws Exception {

        if (paramsMap == null) {
            throw new Exception("please set the args.");
        }

        if (!paramsMap.keySet().contains(key)) {
            throw new Exception("not set " + key + " param.");
        }

        return paramsMap.get(key);
    }
}