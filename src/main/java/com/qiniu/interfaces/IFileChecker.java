package com.qiniu.interfaces;

import java.io.IOException;

public interface IFileChecker {

    String check(String key) throws IOException;
}
