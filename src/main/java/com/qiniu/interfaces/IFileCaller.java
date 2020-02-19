package com.qiniu.interfaces;

import java.io.IOException;
import java.util.List;

public interface IFileCaller<T> {

    String call(List<T> list) throws IOException;
}
