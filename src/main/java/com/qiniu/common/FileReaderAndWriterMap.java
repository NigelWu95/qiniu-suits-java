package com.qiniu.common;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileReaderAndWriterMap implements Cloneable {

    private Map<String, BufferedWriter> WriterMap;
    private Map<String, BufferedReader> ReaderMap;
    private List<String> targetWriters;
    private String targetFileDir;
    private String prefix;

    public FileReaderAndWriterMap() {
        this.targetWriters = Arrays.asList("_success", "_error_null", "_other");
        this.WriterMap = new HashMap<>();
        this.ReaderMap = new HashMap<>();
    }

    public Map<String, BufferedWriter> getWriterMap() {
        return this.WriterMap;
    }

    public Map<String, BufferedReader> getReaderMap() {
        return this.ReaderMap;
    }

    public void initWriter(String targetFileDir, String prefix) throws IOException {
        this.targetFileDir = targetFileDir;
        this.prefix = prefix;

        for (int i = 0; i < targetWriters.size(); i++) {
            addWriter(prefix + targetWriters.get(i));
        }
    }

    public void addWriter(String key) throws IOException {
        File resultFile = new File(targetFileDir, key + ".txt");
        mkDirAndFile(resultFile);
        BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile, true));
        this.WriterMap.put(key, writer);
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

        System.out.println(filePath);
    }

    public BufferedWriter getWriter(String key) {
        return this.WriterMap.get(key);
    }

    public void closeWriter() {
        for (Map.Entry<String, BufferedWriter> entry : this.WriterMap.entrySet()) {
            try {
                this.WriterMap.get(entry.getKey()).close();
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
                this.ReaderMap.put(fileKey.endsWith(".txt") ? fileKey.split("\\.txt")[0] : fileKey, reader);
            }
        }
    }

    public BufferedReader getReader(String key) {
        return this.ReaderMap.get(key);
    }

    public void closeReader() {
        for (Map.Entry<String, BufferedReader> entry : this.ReaderMap.entrySet()) {
            try {
                this.ReaderMap.get(entry.getKey()).close();
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
            System.out.println("Writer " + key + " write {" + item + "} failed");
            ioException.printStackTrace();
        }
    }

    public void writeKeyFile(String key, String item) {
        if (!WriterMap.keySet().contains(key)) {
            try {
                addWriter(key);
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

    public FileReaderAndWriterMap clone() throws CloneNotSupportedException {
        return (FileReaderAndWriterMap)super.clone();
    }
}