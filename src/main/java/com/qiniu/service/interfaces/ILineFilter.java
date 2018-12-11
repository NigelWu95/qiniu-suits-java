package com.qiniu.service.interfaces;


public interface ILineFilter<T> {

    boolean doFilter(T line) throws ReflectiveOperationException;
}
