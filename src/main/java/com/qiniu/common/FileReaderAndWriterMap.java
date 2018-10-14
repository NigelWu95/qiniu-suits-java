package com.qiniu.common;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileReaderAndWriterMap implements Cloneable {

    private HashMap<String, BufferedWriter> writerMap;
    private HashMap<String, BufferedReader> readerMap;
    private List<String> targetWriters;
    private String targetFileDir;
    private String prefix;

    public FileReaderAndWriterMap() {
        this.targetWriters = Arrays.asList("_success", "_error_null", "_other");
        this.writerMap = new HashMap<>();
        this.readerMap = new HashMap<>();
    }

    public Map<String, BufferedWriter> getWriterMap() {
        return this.writerMap;
    }

    public Map<String, BufferedReader> getReaderMap() {
        return this.readerMap;
    }

    public void initWriter(String targetFileDir, String prefix) throws IOException {
        this.targetFileDir = targetFileDir;
        this.prefix = prefix;

        for (int i = 0; i < targetWriters.size(); i++) {
            addWriter(targetFileDir, prefix + targetWriters.get(i));
        }
    }

    public void addWriter(String targetFileDir, String key) throws IOException {
        File resultFile = new File(targetFileDir, key + ".txt");
        mkDirAndFile(resultFile);
        BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile, true));
        this.writerMap.put(key, writer);
    }

    public void mkDirAndFile(File filePath) throws IOException {

        int count = 3;
        while (!filePath.getParentFile().exists()) {
            if (count == 0) {
                throw new IOException("can not make directory.");
            }
            filePath.getParentFile().mkdirs();
            count--;
        }

        count = 3;
        while (!filePath.exists()) {
            if (count == 0) {
                throw new IOException("can not make directory.");
            }
            filePath.createNewFile();
            count--;
        }

        if (count < 3) System.out.println(filePath);
    }

    public BufferedWriter getWriter(String key) {
        return this.writerMap.get(key);
    }

    public void closeWriter() {
        for (Map.Entry<String, BufferedWriter> entry : this.writerMap.entrySet()) {
            try {
                this.writerMap.get(entry.getKey()).close();
            } catch (IOException ioException) {
                System.out.println("Writer " + entry.getKey() + " close failed.");
                ioException.printStackTrace();
            }
        }
    }

    public void initReader(String fileDir) throws IOException {
        File sourceDir = new File(fileDir);
        File[] fs = sourceDir.listFiles();
        String fileKey;
        BufferedReader reader;

        for(File f : fs) {
            if (!f.isDirectory()) {
                FileReader fileReader = new FileReader(f.getAbsoluteFile().getPath());
                reader = new BufferedReader(fileReader);
                fileKey = f.getName();
                this.readerMap.put(fileKey.endsWith(".txt") ? fileKey.split("\\.txt")[0] : fileKey, reader);
            }
        }
    }

    public void initReader(String fileDir, String key) throws IOException {
        File sourceFile = new File(fileDir, key + ".txt");
        FileReader fileReader = new FileReader(sourceFile);
        BufferedReader reader = new BufferedReader(fileReader);
        this.readerMap.put(key.endsWith(".txt") ? key.split("\\.txt")[0] : key, reader);
    }

    public BufferedReader getReader(String key) {
        return this.readerMap.get(key);
    }

    public void closeReader() {
        for (Map.Entry<String, BufferedReader> entry : this.readerMap.entrySet()) {
            try {
                this.readerMap.get(entry.getKey()).close();
            } catch (IOException ioException) {
                System.out.println("Reader " + entry.getKey() + " close failed.");
                ioException.printStackTrace();
            }
        }
    }

    private void doWrite(String key, String item) {
        try {
            getWriter(key).write(item);
            getWriter(key).newLine();
        } catch (IOException ioException) {
            System.out.println("Writer " + key + " write " + item + " failed");
            ioException.printStackTrace();
        }
    }

    public void writeKeyFile(String key, String item) {
        if (!writerMap.keySet().contains(key)) {
            try {
                addWriter(targetFileDir, key);
            } catch (IOException ioException) {
                writeOther(item);
                ioException.printStackTrace();
            }
        }

        doWrite(key, item);
    }

    public void writeSuccess(String item) {
        doWrite(this.prefix + "_success", item);
    }

    public void writeErrorOrNull(String item) {
        doWrite(this.prefix + "_error_null", item);
    }

    public void writeOther(String item) {
        doWrite(this.prefix + "_other", item);
    }
}