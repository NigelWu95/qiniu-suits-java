package com.qiniu.persistence;

import com.qiniu.util.FileNameUtils;

import java.io.*;
import java.util.*;

public class FileSaveMapper implements IResultSave<BufferedWriter> {

    private Map<String, BufferedWriter> writerMap = new HashMap<>();
    private String targetFileDir = null;
    private String prefix = "";
    private String suffix = "";
    private int retryTimes = 5;

    public FileSaveMapper(String targetFileDir) throws IOException {
        this.targetFileDir = FileNameUtils.realPathWithUserHome(targetFileDir);
    }

    public FileSaveMapper(String targetFileDir, String prefix, String suffix) throws IOException {
        this(targetFileDir);
        this.prefix = (prefix == null || "".equals(prefix)) ? "" : prefix + "_";
        this.suffix = (suffix == null || "".equals(suffix)) ? "" : "_" + suffix;
        for (String targetWriter : Collections.singleton("success")) {
            addWriter(this.prefix + targetWriter + this.suffix);
        }
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

    private void addWriter(String key) throws IOException {
        File resultFile = new File(targetFileDir, key + ".txt");
        int retry = retryTimes;
        while (retry > 0) {
            try {
                if (!mkDirAndFile(resultFile)) throw new IOException("create result file " + resultFile + " failed.");
                BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile, true));
                writerMap.put(key, writer);
                retry = 0;
            } catch (IOException e) {
                retry--;
                if (retry <= 0) throw e;
            }

        }
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

    public BufferedWriter getWriter(String key) {
        return writerMap.get(key);
    }

    synchronized public void closeWriters() {
        for (Map.Entry<String, BufferedWriter> entry : writerMap.entrySet()) {
            int retry = retryTimes;
            while (retry > 0) {
                try {
                    if (writerMap.get(entry.getKey()) != null) writerMap.get(entry.getKey()).close();
                    File file = new File(targetFileDir, entry.getKey() + ".txt");
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
    }

    private void writeLine(String key, String item, boolean flush) throws IOException {
        getWriter(key).write(item);
        getWriter(key).newLine();
        if (flush) getWriter(key).flush();
    }

    private void doWrite(String key, String item, boolean flush) throws IOException {
        int count = retryTimes;
        while (count > 0) {
            try {
                writeLine(key, item, flush);
                count = 0;
            } catch (IOException e) {
                count--;
                if (count <= 0) throw e;
            }
        }
    }

    private boolean notHasWriter(String key) {
        return !writerMap.containsKey(prefix + key + suffix);
    }

    // 如果 item 为 null 的话则不进行写入，flush 参数无效
    synchronized public void writeKeyFile(String key, String item, boolean flush) throws IOException {
        if (notHasWriter(key)) addWriter(prefix + key + suffix);
        if (item != null) doWrite(prefix + key + suffix, item, flush);
    }

    synchronized public void writeSuccess(String item, boolean flush) throws IOException {
        if (item != null) doWrite(prefix + "success" + suffix, item, flush);
    }

    private void addErrorWriter() throws IOException {
        addWriter(prefix + "error" + suffix);
    }

    synchronized public void writeError(String item, boolean flush) throws IOException {
        if (notHasWriter("error")) addErrorWriter();
        if (item != null) doWrite(prefix + "error" + suffix, item, flush);
    }
}
