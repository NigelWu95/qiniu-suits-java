package com.qiniu.service.interfaces;

import java.util.List;

public interface ILineFilter<T> {

    boolean doFilter(T line) throws Exception;

    List<T> check(List<T> lineList) throws Exception;
}
