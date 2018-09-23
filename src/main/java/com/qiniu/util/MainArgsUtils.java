package com.qiniu.util;

public class MainArgsUtils {

    private static String[] params = new String[]{};

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
}