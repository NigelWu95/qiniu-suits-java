package com.qiniu.common;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileReaderAndWriterMap {

    private Map<String, OutputStreamWriter> outputStreamWriterMap;
    private Map<String, InputStreamReader> inputStreamReaderMap;
    private List<String> targetWriters;
    private String targetFileDir;
    private String prefix;

    public FileReaderAndWriterMap() {
        this.targetWriters = Arrays.asList("_success", "_error_null", "_other");
        this.outputStreamWriterMap = new HashMap<>();
        inputStreamReaderMap = new HashMap<>();
    }

    public Map<String, OutputStreamWriter> getOutputStreamWriterMap() {
        return this.outputStreamWriterMap;
    }

    public Map<String, InputStreamReader> getInputStreamReaderMap() {
        return this.inputStreamReaderMap;
    }

    public void initOutputStreamWriter(String targetFileDir, String prefix) throws IOException {
        this.targetFileDir = targetFileDir;
        this.prefix = prefix;

        for (int i = 0; i < targetWriters.size(); i++) {
            addWriter(prefix + targetWriters.get(i));
        }
    }

    public void addWriter(String key) throws IOException {
        File resultFile = new File(targetFileDir, key + ".txt");
        mkDirAndFile(resultFile);
        OutputStreamWriter resultFileWriter = new OutputStreamWriter(new FileOutputStream(resultFile, true), "UTF-8");
        this.outputStreamWriterMap.put(key, resultFileWriter);
    }

    public void mkDirAndFile(File filePath) throws IOException {

        while (!filePath.getParentFile().exists()) {
            filePath.getParentFile().mkdirs();
        }

        while (!filePath.exists()) {
            filePath.createNewFile();
        }
    }

    public OutputStreamWriter getOutputStreamWriter(String key) {
        return this.outputStreamWriterMap.get(key);
    }

    public void closeStreamWriter() {
        for (Map.Entry<String, OutputStreamWriter> entry : this.outputStreamWriterMap.entrySet()) {
            try {
                this.outputStreamWriterMap.get(entry.getKey()).close();
            } catch (IOException ioException) {
                System.out.println("OutputStreamWriter " + entry.getKey() + " close failed.");
                ioException.printStackTrace();
            }
        }
    }

    public void initInputStreamReader(String fileDir) throws IOException {
        File sourceDir = new File(fileDir);
        File[] fs = sourceDir.listFiles();
        String fileKey = "";
        InputStreamReader fileReader = null;

        for(File f : fs) {
            if (!f.isDirectory()) {
                FileInputStream fileInputStream = new FileInputStream(f.getAbsoluteFile().getPath());
                fileReader = new InputStreamReader(fileInputStream, "UTF-8");
                fileKey = f.getName();
                this.inputStreamReaderMap.put(fileKey.endsWith(".txt") ? fileKey.split("\\.txt")[0] : fileKey, fileReader);
            }
        }
    }

    public InputStreamReader getInputStreamReader(String key) {
        return this.inputStreamReaderMap.get(key);
    }

    public void closeStreamReader() {
        for (Map.Entry<String, InputStreamReader> entry : this.inputStreamReaderMap.entrySet()) {
            try {
                this.inputStreamReaderMap.get(entry.getKey()).close();
            } catch (IOException ioException) {
                System.out.println("InputStreamReader " + entry.getKey() + " close failed.");
                ioException.printStackTrace();
            }
        }
    }

    private void doWrite(String key, String item) {
        try {
            getOutputStreamWriter(key).write(item + "\n");
            getOutputStreamWriter(key).flush();
        } catch (IOException ioException) {
            System.out.println("OutputStreamWriter " + key + " write {" + item + "} failed");
            ioException.printStackTrace();
        }
    }

    public void writeKeyFile(String key, String item) {
        if (!outputStreamWriterMap.keySet().contains(key)) {
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

    public void writeErrorAndNull(String item) {
        doWrite(this.prefix + "_error_null", item);
    }

    public void writeOther(String item) {
        doWrite(this.prefix + "_other", item);
    }
}