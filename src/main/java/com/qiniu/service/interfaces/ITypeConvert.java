package com.qiniu.service.interfaces;

import java.util.ArrayList;
import java.util.List;

public interface ITypeConvert<E, M> {

    List<M> convertToVList(List<E> srcList);

    // 获得完整的 errorList，可以多次调用
    default List<String> getErrorList() {
        return new ArrayList<>();
    }

    // 消费者方法，调用后返回列表并清空原列表中
    default List<String> consumeErrorList() {
        return new ArrayList<>();
    }
}
