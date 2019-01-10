package com.qiniu.service.interfaces;

import java.util.List;

public interface ITypeConvert<T, V> {

    List<V> convertToVList(List<T> srcList);

    // 获得完整的 errorList，可以多次调用
    List<String> getErrorList();

    // 消费者方法，调用后返回列表并清空原列表中
    List<String> consumeErrorList();
}
