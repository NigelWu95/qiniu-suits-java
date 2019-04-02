package com.qiniu.persistence;

import com.qiniu.util.FileNameUtils;

import java.io.*;
import java.util.*;
import java.util.Map.*;

public class FileMap {

    private HashMap<String, BufferedWriter> writerMap;
    private HashMap<String, BufferedReader> readerMap;
    private Set<String> defaultWriters;
    private String targetFileDir = null;
    private String prefix = null;
    private String suffix = null;
    private int retryTimes = 3;

    public FileMap() {
        this.defaultWriters = Collections.singleton("success");
        this.writerMap = new HashMap<>();
        this.readerMap = new HashMap<>();
    }

    public FileMap(String targetFileDir) {
        this();
        this.targetFileDir = FileNameUtils.realPathWithUserHome(targetFileDir);
        this.prefix = "";
        this.suffix = "";
    }

    public FileMap(String targetFileDir, String prefix, String suffix) {
        this(targetFileDir);
        this.prefix = (prefix == null || "".equals(prefix)) ? "" : prefix + "_";
        this.suffix = (suffix == null || "".equals(suffix)) ? "" : "_" + suffix;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes < 1 ? 3 : retryTimes;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getSuffix() {
        return suffix;
    }

    public void addDefaultWriters(String writer) throws IOException {
        if (writer == null || "".equals(writer)) throw new IOException("not valid writer.");
        defaultWriters.add(writer);
    }

    synchronized public void initDefaultWriters() throws IOException {
        if (targetFileDir == null || "".equals(targetFileDir)) throw new IOException("no result file directory.");
        for (String targetWriter : defaultWriters) {
            addWriter(prefix + targetWriter + suffix);
        }
    }

    public void initDefaultWriters(String targetFileDir, String prefix, String suffix) throws IOException {
        if (targetFileDir != null && !"".equals(targetFileDir)) this.targetFileDir = targetFileDir;
        if (prefix != null && !"".equals(prefix)) this.prefix = prefix + "_";
        else if (this.prefix == null) this.prefix = "";
        if (suffix != null && !"".equals(suffix)) this.suffix = "_" + suffix;
        else if (this.suffix == null) this.suffix = "";
        initDefaultWriters();
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

    public void initReaders(String fileDir) throws IOException {
        fileDir = FileNameUtils.realPathWithUserHome(fileDir);
        File sourceDir = new File(fileDir);
        File[] fs = sourceDir.listFiles();
        String fileName;
        String key;
        BufferedReader reader;
        if (fs == null) throw new IOException("The current path you gave may be incorrect: " + fileDir);
        for(File f : fs) {
            if (!f.isDirectory()) {
                FileReader fileReader = new FileReader(f.getAbsoluteFile().getPath());
                reader = new BufferedReader(fileReader);
                fileName = f.getName();
                if (fileName.endsWith(".txt")) {
                    key = fileName.substring(0, fileName.length() - 4);
                    if (readerMap.containsKey(key)) throw new IOException("the reader: " + key + " is already init.");
                    readerMap.put(key, reader);
                }
            }
        }
        if (readerMap.size() == 0) throw new IOException("please provide the .txt file int the directory. The current" +
                " path you gave is: " + fileDir);
    }

    public void initReader(String filepath) throws IOException {
        if (filepath.endsWith(".txt")) {
            filepath = FileNameUtils.realPathWithUserHome(filepath);
            File sourceFile = new File(filepath);
            FileReader fileReader;
            try {
                fileReader = new FileReader(sourceFile);
            } catch (IOException e) {
                throw new IOException("file-path parameter may be incorrect, " + e.getMessage());
            }
            BufferedReader reader = new BufferedReader(fileReader);
            String key = sourceFile.getName().substring(0, sourceFile.getName().length() - 4);
            if (readerMap.containsKey(key)) throw new IOException("the reader: " + key + " is already init.");
            readerMap.put(key, reader);
        } else {
            throw new IOException("please provide the .txt file. The current path you gave is: " + filepath);
        }
    }

    public BufferedReader getReader(String key) {
        return readerMap.get(key);
    }

    public HashMap<String, BufferedReader> getReaderMap() {
        return readerMap;
    }

    synchronized public void closeReaders() {
        for (Entry<String, BufferedReader> entry : readerMap.entrySet()) {
            try {
                if (readerMap.get(entry.getKey()) != null) readerMap.get(entry.getKey()).close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    synchronized public void closeReader(String key) {
        try {
            if (readerMap.get(key) != null) readerMap.get(key).close();
        } catch (IOException e) {
            e.printStackTrace();
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

    synchronized private boolean notHasWriter(String key) {
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
