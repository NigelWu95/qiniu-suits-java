package com.qiniu.interfaces;

public interface ILineFilter<T> {

    boolean doFilter(T line) throws Exception;
}
