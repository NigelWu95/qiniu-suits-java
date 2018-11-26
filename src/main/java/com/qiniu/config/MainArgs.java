package com.qiniu.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MainArgs {

    private String[] params = new String[]{};
    private Map<String, String> paramsMap = new HashMap<>();

    public MainArgs(String[] args) throws IOException {
        setParamsMap(args);
    }

    private void setParams(String[] args) throws IOException {

        if (args != null) {
            if (params.length == args.length)
                return;
            params = new String[args.length];
            System.arraycopy(args, 0, params, 0, args.length);
        }
        else
            throw new IOException("args is null");
    }

    public String getParam(String[] args, int index, String error) throws IOException {

        try {
            setParams(args);
        } catch (IOException e) {
            throw new IOException(e.getMessage() + ", " + error);
        }

        if (index < 1) {
            throw new IOException("index must be more than 1.");
        }

        if (args.length < index) {
            throw new IOException("index is out of bound, " + error);
        }

        System.out.println(params[index - 1]);
        return params[index - 1];
    }

    private static String[] splitParam(String paramCommand) throws IOException {

        if (!paramCommand.contains("=") || !paramCommand.startsWith("-")) {
            throw new IOException("it is invalid command param.");
        }

        String[] strings = paramCommand.substring(1).split("=");

        if (strings.length == 1) {
            throw new IOException("the " + strings[0] + " param has no value.");
        }

        return strings;
    }

    public void setParamsMap(String[] args) throws IOException {

        if (args != null) {
            if (paramsMap.size() == args.length)
                return;

            for (String arg : args) {
                paramsMap.put(splitParam(arg)[0], splitParam(arg)[1]);
            }
        }
        else
            throw new IOException("args is null");
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
}
