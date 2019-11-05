package com.qiniu.interfaces;

import java.io.IOException;

public interface IStringFormat<T> {

    String toFormatString(T t) throws IOException;
}
