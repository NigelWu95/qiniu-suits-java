package com.qiniu.datasource;

import com.qiniu.interfaces.IReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.Stream;

public class LocalFileReader implements IReader<BufferedReader> {

    private String name;
    private BufferedReader bufferedReader;

    public LocalFileReader(File sourceFile) throws IOException {
        FileReader fileReader;
        try {
            fileReader = new FileReader(sourceFile);
        } catch (IOException e) {
            throw new IOException("file-path parameter may be incorrect, " + e.getMessage());
        }
        name = sourceFile.getPath();
        bufferedReader = new BufferedReader(fileReader);
    }

    public String getName() {
        return name;
    }

    public BufferedReader getRealReader() {
        return bufferedReader;
    }

    public String readLine() throws IOException {
        return bufferedReader.readLine();
    }

    public Stream<String> lines() {
        return bufferedReader.lines();
    }

    public void close() {
        try {
            bufferedReader.close();
        } catch (IOException e) {
            bufferedReader = null;
        }
    }
}
