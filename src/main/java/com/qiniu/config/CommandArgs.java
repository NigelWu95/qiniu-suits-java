package com.qiniu.config;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class CommandArgs implements IEntryParam {

    private String[] params;
    private Map<String, String> paramsMap;

    public CommandArgs(String[] args) throws IOException {
        if (args == null || args.length == 0)
            throw new IOException("args is null.");
        else {
            int cmdCount = 0;
            boolean cmdGoon = true;
            paramsMap = new HashMap<>();
            for (String arg : args) {
                // "-" 开头的参数之前放置到 params 数组中
                if (!arg.contains("=") && !arg.startsWith("-") && cmdGoon) cmdCount++;
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
            throw new IOException("there is invalid command param: \"" + paramCommand + "\".");
        }

        String[] strings = paramCommand.substring(1).split("=");
        if (strings.length == 1) {
            // 不允许空值的出现
            throw new IOException("the \"" + strings[0] + "\" param has no value.");
        }

        if (strings[1].matches("(\".*\"|\'.*\')"))
            return new String[]{strings[0], strings[1].substring(1, strings[1].length() -1)};
        return strings;
    }

    /**
     * 获取属性值，判断是否存在相应的 key，不存在或 value 为空则抛出异常
     * @param key 属性名
     * @return 属性值字符
     * @throws IOException
     */
    public String getValue(String key) throws IOException {
        if (paramsMap == null) {
            throw new IOException("please set the args.");
        }

        if (paramsMap.containsKey(key)) {
            return paramsMap.get(key);
        } else {
            throw new IOException("not set \"" + key + "\" parameter.");
        }

    }

    /**
     * 获取属性值，不抛出异常，使用 default 值进行返回
     * @param key
     * @param Default 默认返回值
     * @return 属性值字符
     */
    public String getValue(String key, String Default) {
        if ("".equals(paramsMap.get(key))) return Default;
        return paramsMap.getOrDefault(key, Default);
    }

    /**
     * 获取属性值，通过反射转换成指定类型
     * @param key
     * @param clazz 返回值类型 class
     * @param Default
     * @param <T> 范型
     * @return
     * @throws Exception
     */

    public <T> T getValue(String key, Class<T> clazz, T Default) throws Exception {
        if (paramsMap.containsKey(key) && !"".equals(paramsMap.get(key))) {
            return (T) clazz.getMethod("valueOf", clazz.getClasses()).invoke(clazz, paramsMap.get(key));
        } else {
            return Default;
        }
    }

    public String[] getParams() {
        return params;
    }
}
