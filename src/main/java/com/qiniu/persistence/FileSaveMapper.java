package com.qiniu.persistence;

import com.qiniu.util.FileUtils;

import java.io.*;
import java.util.*;

public class FileSaveMapper implements IResultOutput<BufferedWriter> {

    private Map<String, BufferedWriter> writerMap = new HashMap<>();
    private String targetFileDir = null;
    private String prefix = "";
    private String suffix = "";
    public static String ext = ".txt";
    public static boolean append = true;
    private int retryTimes = 5;

    public FileSaveMapper(String targetFileDir) throws IOException {
        this.targetFileDir = FileUtils.realPathWithUserHome(targetFileDir);
    }

    public FileSaveMapper(String targetFileDir, String prefix, String suffix) throws IOException {
        this(targetFileDir);
        this.prefix = (prefix == null || "".equals(prefix)) ? "" : prefix + "_";
        this.suffix = (suffix == null || "".equals(suffix)) ? "" : "_" + suffix;
        for (String targetWriter : "success,error".split(",")) addWriter(targetWriter);
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes < 1 ? 5 : retryTimes;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getSuffix() {
        return suffix;
    }

    synchronized public void addWriter(String key) throws IOException {
        BufferedWriter writer = writerMap.get(key);
        if (writer != null) throw new IOException("this writer is already exists.");
        File resultFile = new File(targetFileDir, prefix + key + this.suffix + ext);
        int retry = retryTimes;
        while (retry > 0) {
            try {
                if (!mkDirAndFile(resultFile)) throw new IOException("create result file " + resultFile + " failed.");
                writer = new BufferedWriter(new FileWriter(resultFile, append));
                writerMap.put(key, writer);
                retry = 0;
            } catch (IOException e) {
                retry--;
                if (retry <= 0) throw e;
            }

        }
    }

    synchronized public void addWriters(List<String> writers) throws IOException {
        for (String targetWriter : writers) addWriter(targetWriter);
    }

    private boolean mkDirAndFile(File filePath) throws IOException {
        boolean success = filePath.getParentFile().exists();
        if (!success) {
            success = filePath.getParentFile().mkdirs();
            if (!success) return false;
        }
        success = filePath.exists();
        if (!success) {
            return filePath.createNewFile();
        } else {
            return true;
        }
    }

    synchronized public void closeWriters() {
        if (writerMap.size() <= 0) return;
        int retry;
        BufferedWriter bufferedWriter;
        for (Map.Entry<String, BufferedWriter> entry : writerMap.entrySet()) {
            retry = retryTimes;
            while (retry > 0) {
                try {
                    bufferedWriter = writerMap.get(entry.getKey());
                    if (bufferedWriter != null) bufferedWriter.close();
                    File file = new File(targetFileDir, prefix + entry.getKey() + this.suffix + ext);
                    if (file.exists()) {
                        BufferedReader reader = new BufferedReader(new FileReader(file));
                        if (reader.readLine() == null) {
                            reader.close();
                            if (file.delete()) retry = 0;
                        } else {
                            retry = 0;
                        }
                    } else {
                        retry = 0;
                    }
                } catch (IOException e) {
                    retry--;
                    if (retry <= 0) e.printStackTrace();
                }
            }
        }
        writerMap.clear();
        targetFileDir = null;
        prefix = null;
        suffix = null;
    }

    synchronized private void doWrite(String key, String item, boolean flush) throws IOException {
        int count = retryTimes;
        BufferedWriter bufferedWriter = writerMap.get(key);
        if (bufferedWriter != null) {
            while (count > 0) {
                try {
                    bufferedWriter.write(item);
                    bufferedWriter.newLine();
                    if (flush) bufferedWriter.flush();
                    count = 0;
                } catch (IOException e) {
                    count--;
                    if (count <= 0) throw e;
                }
            }
        } else {
            throw new IOException("the writer is not exists now.");
        }
    }

    // 如果 item 为 null 的话则不进行写入，flush 参数无效
    public void writeToKey(String key, String item, boolean flush) throws IOException {
        if (item != null) doWrite(key, item, flush);
        else throw new IOException("can't write empty.");
    }

    public void writeSuccess(String item, boolean flush) throws IOException {
        if (item != null) doWrite("success", item, flush);
        else throw new IOException("can't write empty.");
    }

    public void writeError(String item, boolean flush) throws IOException {
        if (item != null) doWrite("error", item, flush);
        else throw new IOException("can't write empty.");
    }
}
