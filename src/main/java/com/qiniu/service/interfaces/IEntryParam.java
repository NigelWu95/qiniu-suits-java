package com.qiniu.service.interfaces;

import java.io.IOException;

public interface IEntryParam {

    /**
     * 获取属性值，判断是否存在相应的 key，否则抛出异常
     * @param key 属性名
     * @return 属性值字符
     * @throws IOException
     */
    String getParamValue(String key) throws IOException;

    /**
     * 获取属性值，不抛出异常，使用 default 值进行返回
     * @param key
     * @param defaultValue 默认返回值
     * @return 属性值字符
     */
    String getParamValue(String key, String defaultValue);

    /**
     * 获取属性值，通过反射转换成指定类型
     * @param key
     * @param clazz 返回值类型 class
     * @param defaultValue
     * @param <T> 范型
     * @return
     * @throws Exception
     */
    <T> T getParamValue(String key, Class<T> clazz, T defaultValue) throws Exception;
}
