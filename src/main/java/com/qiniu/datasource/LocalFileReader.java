package com.qiniu.datasource;

import java.io.BufferedReader;

public class LocalFileReader implements IReader {

    private String name;
    private BufferedReader bufferedReader;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public BufferedReader getBufferedReader() {
        return bufferedReader;
    }

    public void setBufferedReader(BufferedReader bufferedReader) {
        this.bufferedReader = bufferedReader;
    }
}
