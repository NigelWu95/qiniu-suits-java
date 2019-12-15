package com.qiniu.persistence;

import com.qiniu.interfaces.IResultOutput;
import com.qiniu.util.FileUtils;

import java.io.*;
import java.util.*;

public class FileSaveMapper implements IResultOutput {

    private Map<String, BufferedWriter> writerMap = new HashMap<>();
    private String savePath;
    private String prefix = "";
    private String suffix = "";
    private String fileExt = ".txt";
    private boolean append = true;
    private int retryTimes = 5;

    public FileSaveMapper(String savePath) throws IOException {
        this.savePath = FileUtils.convertToRealPath(savePath);
        File fDir = new File(this.savePath);
        int retry = retryTimes;
        while (retry > 0 && !fDir.exists()) {
            try {
                if (!fDir.mkdirs() && !fDir.exists()) throw new IOException("create result directory: " + savePath + " failed.");
                retry = 0;
            } catch (IOException e) {
                retry--;
                if (retry <= 0) throw e;
            }
        }
    }

    public FileSaveMapper(String savePath, String prefix, String suffix) throws IOException {
        this(savePath);
        this.prefix = (prefix == null || "".equals(prefix)) ? "" : String.join("", prefix, "_");
        this.suffix = (suffix == null || "".equals(suffix)) ? "" : String.join("", "_", suffix);
        for (String targetWriter : "success,error".split(",")) preAddWriter(targetWriter);
    }

    public void changePrefixAndSuffix(String prefix, String suffix) {
        if (prefix != null && !"".equals(prefix)) {
            this.prefix = String.join("", prefix, "_");
        }
        if (suffix != null && !"".equals(suffix)) {
            this.suffix = String.join("", "_", suffix);
        }
        if (!writerMap.containsKey("success")) {
            for (String targetWriter : "success,error".split(",")) preAddWriter(targetWriter);
        }
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes < 1 ? 5 : retryTimes;
    }

    public void setFileExt(String fileExt) {
        this.fileExt = fileExt;
    }

    public void setAppend(boolean append) {
        this.append = append;
    }

    public String getSavePath() {
        return savePath;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getSuffix() {
        return suffix;
    }

    public void preAddWriter(String key) {
        writerMap.put(key, null);
    }

    private BufferedWriter add(String key) throws IOException {
        File resultFile = new File(savePath, String.join("", prefix, key, suffix, fileExt));
        boolean resultFileExists = resultFile.exists();
        int retry = retryTimes;
        BufferedWriter writer = null;
        while (retry > 0) {
            try {
                if (!resultFileExists) {
                    resultFileExists = resultFile.createNewFile();
                    if (!resultFileExists) throw new IOException("create result file " + resultFile + " failed.");
                }
                writer = new BufferedWriter(new FileWriter(resultFile, append));
                retry = 0;
            } catch (IOException e) {
                retry--;
                if (retry <= 0) throw e;
            }
        }
        return writer;
    }

    public synchronized void addWriter(String key) throws IOException {
        BufferedWriter writer = writerMap.get(key);
        if (writer != null) throw new IOException("this writer is already exists.");
        writer = add(key);
        writerMap.put(key, writer);
    }

    public synchronized void closeWriters() {
        if (writerMap.size() <= 0) return;
        int retry;
        BufferedWriter bufferedWriter;
        for (Map.Entry<String, BufferedWriter> entry : writerMap.entrySet()) {
            retry = retryTimes;
            while (retry > 0) {
                try {
                    bufferedWriter = writerMap.get(entry.getKey());
                    if (bufferedWriter != null) bufferedWriter.close();
                    File file = new File(savePath, String.join("", prefix, entry.getKey(), suffix, fileExt));
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
        savePath = null;
        prefix = null;
        suffix = null;
    }

    private synchronized void doWrite(String key, String item, boolean flush) throws IOException {
        BufferedWriter bufferedWriter = writerMap.get(key);
        if (bufferedWriter == null) {
            if (writerMap.containsKey(key)) {
                bufferedWriter = add(key);
                writerMap.put(key, bufferedWriter);
            } else {
                throw new IOException("the writer is not exists now.");
            }
        }
        int retry = retryTimes;
        while (retry > 0) {
            try {
                bufferedWriter.write(item);
                bufferedWriter.newLine();
                if (flush) bufferedWriter.flush();
                retry = 0;
            } catch (IOException e) {
                retry--;
                if (retry <= 0) throw e;
            }
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
