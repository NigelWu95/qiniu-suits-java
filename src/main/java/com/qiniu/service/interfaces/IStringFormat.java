package com.qiniu.service.interfaces;

import java.io.IOException;
import java.util.List;

public interface IStringFormat<T> {

    String toFormatString(T t) throws IOException;
}
