package com.qiniu.datasource;

import com.qiniu.util.FileNameUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.Stream;

public class LocalFileReader implements IReader<BufferedReader> {

    private String name;
    private BufferedReader bufferedReader;

    public LocalFileReader(String filepath) throws IOException {
        if (filepath.endsWith(".txt")) {
            filepath = FileNameUtils.realPathWithUserHome(filepath);
            File sourceFile = new File(filepath);
            FileReader fileReader;
            try {
                fileReader = new FileReader(sourceFile);
            } catch (IOException e) {
                throw new IOException("file-path parameter may be incorrect, " + e.getMessage());
            }
            bufferedReader = new BufferedReader(fileReader);
            name = sourceFile.getName().substring(0, sourceFile.getName().length() - 4);
        } else {
            throw new IOException("please provide the .txt file. The current path you gave is: " + filepath);
        }
    }

    public String getName() {
        return name;
    }

    public BufferedReader getRealReader() {
        return bufferedReader;
    }

    public String read() throws IOException {
        return bufferedReader.readLine();
    }

    public Stream<String> lines() {
        return bufferedReader.lines();
    }
}
