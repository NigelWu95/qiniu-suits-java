package com.qiniu.persistence;

import java.io.*;
import java.util.*;
import java.util.Map.*;

public class FileMap {

    private HashMap<String, BufferedWriter> writerMap;
    private HashMap<String, BufferedReader> readerMap;
    private List<String> defaultWriters;
    private String targetFileDir = null;
    private String prefix = null;
    private String suffix = null;
    private int retryCount = 3;

    public FileMap() {
        this.defaultWriters = Collections.singletonList("success");
        this.writerMap = new HashMap<>();
        this.readerMap = new HashMap<>();
    }

    public FileMap(String targetFileDir) {
        this();
        this.targetFileDir = targetFileDir;
        this.prefix = "";
        this.suffix = "";
    }

    public FileMap(String targetFileDir, String prefix, String suffix) {
        this(targetFileDir);
        this.prefix = (prefix == null || "".equals(prefix)) ? "" : prefix + "_";
        this.suffix = (suffix == null || "".equals(suffix)) ? "" : "_" + suffix;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount < 1 ? 1 : retryCount;
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

    public void initDefaultWriters() throws IOException {
        if (targetFileDir == null || "".equals(targetFileDir)) throw new IOException("no target file directory.");
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
        int retry = retryCount;
        while (retry > 0) {
            try {
                if (!mkDirAndFile(resultFile)) throw new IOException("create file failed.");
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

    public void closeWriters() {
        for (Map.Entry<String, BufferedWriter> entry : writerMap.entrySet()) {
            int retry = retryCount;
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
        File sourceDir = new File(fileDir);
        File[] fs = sourceDir.listFiles();
        String fileName;
        BufferedReader reader;
        assert fs != null;
        for(File f : fs) {
            if (!f.isDirectory()) {
                FileReader fileReader = new FileReader(f.getAbsoluteFile().getPath());
                reader = new BufferedReader(fileReader);
                fileName = f.getName();
                if (fileName.endsWith(".txt")) readerMap.put(fileName.substring(0, fileName.length() - 4), reader);
            }
        }
        if (readerMap.size() == 0) throw new IOException("please provide the .txt file int the directory.");
    }

    public void initReader(String fileDir, String fileName) throws IOException {
        if (fileName.endsWith(".txt")) {
            File sourceFile = new File(fileDir, fileName);
            FileReader fileReader = new FileReader(sourceFile);
            BufferedReader reader = new BufferedReader(fileReader);
            readerMap.put(fileName.substring(0, fileName.length() - 4), reader);
        } else {
            throw new IOException("please provide the .txt file.");
        }
    }

    public BufferedReader getReader(String key) {
        return readerMap.get(key);
    }

    public HashMap<String, BufferedReader> getReaderMap() {
        return readerMap;
    }

    public void closeReaders() {
        for (Entry<String, BufferedReader> entry : readerMap.entrySet()) {
            try {
                if (readerMap.get(entry.getKey()) != null) readerMap.get(entry.getKey()).close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void closeReader(String key) {
        try {
            if (readerMap.get(key) != null) readerMap.get(key).close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeLine(String key, String item) throws IOException {
        getWriter(key).write(item);
        getWriter(key).newLine();
    }

    private void doWrite(String key, String item) {
        int count = retryCount;
        while (count > 0) {
            try {
                writeLine(key, item);
                count = 0;
            } catch (IOException e) {
                count--;
                if (count <= 0) e.printStackTrace();
            }
        }
    }

    public void writeKeyFile(String key, String item) throws IOException {
        if (!writerMap.keySet().contains(prefix + key + suffix)) addWriter(prefix + key + suffix);
        doWrite(prefix + key + suffix, item);
    }

    public void writeSuccess(String item) {
        doWrite(prefix + "success" + suffix, item);
    }

    public void addErrorWriter() throws IOException {
        addWriter(prefix + "error" + suffix);
    }

    public void writeError(String item) throws IOException {
        if (!writerMap.keySet().contains(prefix + "error" + suffix)) addErrorWriter();
        doWrite(prefix + "error" + suffix, item);
    }
}
