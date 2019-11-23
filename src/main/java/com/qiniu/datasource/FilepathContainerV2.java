package com.qiniu.datasource;

import com.google.gson.JsonObject;
import com.qiniu.convert.Converter;
import com.qiniu.convert.MapToString;
import com.qiniu.convert.StringMapPair;
import com.qiniu.interfaces.IReader;
import com.qiniu.interfaces.IResultOutput;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.model.local.FileInfo;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.util.ConvertingUtils;
import com.qiniu.util.FileUtils;
import com.qiniu.util.JsonUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class FilepathContainerV2 extends FileContainer<FileInfo, BufferedWriter, Map<String, String>> {

    private String transferPath = null;
    private int leftTrimSize = 0;
    private String realPath;
    private List<String> antiDirectories;
    private boolean hasAntiDirectories = false;
    private Map<String, Map<String, String>> directoriesMap;
    private List<File> directories;
    private List<FileInfo> originalFileInfos;

    public FilepathContainerV2(String path, String parseFormat, String separator, Map<String, Map<String, String>> directoriesMap,
                               List<String> antiDirectories, String addKeyPrefix, String rmKeyPrefix, Map<String, String> indexMap,
                               List<String> fields, int unitLen, int threads) throws IOException {
        super(path, parseFormat, separator, addKeyPrefix, rmKeyPrefix, null, indexMap, fields, unitLen, threads);
        this.antiDirectories = antiDirectories;
        if (antiDirectories != null && antiDirectories.size() > 0) hasAntiDirectories = true;
        setTransferPathAndLeftTrimSize();
        setDirectoriesAndMap(directoriesMap);
    }

    private void setTransferPathAndLeftTrimSize() throws IOException {
        if (path.indexOf(FileUtils.pathSeparator + FileUtils.currentPath) > 0 ||
                path.indexOf(FileUtils.pathSeparator + FileUtils.parentPath) > 0 ||
                path.endsWith(FileUtils.pathSeparator + ".") ||
                path.endsWith(FileUtils.pathSeparator + "..")) {
            throw new IOException("please set straight path.");
        } else if (path.startsWith(FileUtils.userHomeStartPath)) {
            realPath = String.join("", FileUtils.userHome, path.substring(1));
            transferPath = "~";
            leftTrimSize = FileUtils.userHome.length();
        } else {
            realPath = path;
            if (path.startsWith(FileUtils.parentPath) || "..".equals(path)) {
                transferPath = "";
                leftTrimSize = 3;
            } else if (path.startsWith(FileUtils.currentPath) || ".".equals(path)) {
                transferPath = "";
                leftTrimSize = 2;
            }
        }
        if (realPath.contains("\\~")) realPath = realPath.replace("\\~", "~");
        if (realPath.endsWith(FileUtils.pathSeparator)) {
            realPath = realPath.substring(0, realPath.length() - 1);
        }
    }

    private void setDirectoriesAndMap(Map<String, Map<String, String>> directoriesMap) throws IOException {
        if (directoriesMap == null || directoriesMap.size() <= 0) {
            this.directoriesMap = new HashMap<>(threads);
            if (hasAntiDirectories) {
                FilepathLister filepathLister = new FilepathLister(new File(realPath), true, transferPath, leftTrimSize);
                originalFileInfos = filepathLister.getFileInfos();
                directories = filepathLister.getDirectories();
            }
        } else {
            if (directoriesMap.containsKey(null)) throw new IOException("can not find directory named \"null\".");
            this.directoriesMap = new HashMap<>(threads);
            this.directoriesMap.putAll(directoriesMap);
            List<String> list = this.directoriesMap.keySet().stream().sorted().collect(Collectors.toList());
            int size = list.size();
            Iterator<String> iterator = list.iterator();
            directories = new ArrayList<>();
            String temp = iterator.next();
            File tempFile;
            Map<String, String> value = directoriesMap.get(temp);
            String end;
            if (temp == null || temp.equals("")) {
                throw new IOException("directories can not contains empty item");
            } else {
                tempFile = new File(temp);
                if (tempFile.exists() && tempFile.isDirectory()) directories.add(tempFile);
            }
            while (iterator.hasNext() && size > 0) {
                size--;
                String directory = iterator.next();
                if (directory == null || directory.equals("")) {
                    throw new IOException("directories can not contains empty item");
                } else {
                    File file = new File(directory);
                    if (file.isDirectory()) {
                        if (tempFile.isDirectory()) {
                            if (file.getPath().startsWith(tempFile.getPath())) {
                                end = value == null ? null : value.get("end");
                                if (end == null || "".equals(end)) {
                                    iterator.remove();
                                    this.directoriesMap.remove(directory);
                                } else if (end.compareTo(directory) >= 0) {
                                    throw new IOException(temp + "'s end can not be more larger than " + directory + " in " + directoriesMap);
                                } else {
                                    directories.add(file);
                                }
                            } else {
                                directories.add(file);
                            }
                        } else {
                            directories.add(file);
                            tempFile = file;
                            temp = directory;
                            value = directoriesMap.get(temp);
                        }

                    }
                }
            }
        }
    }

    boolean checkDirectory(File directory) {
        if (hasAntiDirectories) {
            for (String antiPrefix : antiDirectories) {
                if (directory.getPath().startsWith(antiPrefix)) return false;
            }
            return true;
        } else {
            return true;
        }
    }

    @Override
    protected ITypeConvert<FileInfo, Map<String, String>> getNewConverter() throws IOException {
        return new Converter<FileInfo, Map<String, String>>() {
            @Override
            public Map<String, String> convertToV(FileInfo line) throws IOException {
                return ConvertingUtils.toPair(line, indexMap, new StringMapPair());
            }
        };
    }

    @Override
    protected ITypeConvert<Map<String, String>, String> getNewStringConverter() throws IOException {
        return new MapToString(saveFormat, saveSeparator, fields);
    }

    @Override
    public String getSourceName() {
        return "filepath";
    }

    @Override
    protected IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    @Override
    protected List<IReader<FileInfo>> getFileReaders(String path) throws IOException {
        return null;
    }

    void recordListerByDirectory(String directory) {
        JsonObject json = directoriesMap.get(directory) == null ? null : JsonUtils.toJsonObject(directoriesMap.get(directory));
        try { FileUtils.createIfNotExists(procedureLogFile); } catch (IOException ignored) {}
        procedureLogger.info(recorder.put(directory, json));
    }

    private List<File> directoriesAfterListerRun(File directory) {
        try {
            FilepathLister filepathLister = new FilepathLister(directory, true, transferPath, leftTrimSize);
            FileInfoReader fileInfoReader = new FileInfoReader(path, filepathLister.getFileInfos(), filepathLister.getFileInfos().get(0), unitLen);
            if (fileInfoReader.lastLine() != null || filepathLister.getDirectories() != null) {
                reading(fileInfoReader);
                if (filepathLister.getDirectories() == null || filepathLister.getDirectories().size() <= 0) {
                    return null;
                } else if (hasAntiDirectories) {
                    return filepathLister.getDirectories().stream().filter(this::checkDirectory)
                            .peek(dir -> recordListerByDirectory(dir.getPath())).collect(Collectors.toList());
                } else {
                    for (File dir : filepathLister.getDirectories()) recordListerByDirectory(dir.getPath());
                    return filepathLister.getDirectories();
                }
            } else {
                reading(fileInfoReader);
                return filepathLister.getDirectories();
            }
        } catch (IOException e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("generate lister failed by {}\t{}", directory.getPath(), directoriesMap.get(directory.getPath()), e);
            return null;
        }
    }

    private AtomicLong atomicLong = new AtomicLong(0);

    private void listForNextIteratively(List<File> directories) throws Exception {
        List<File> tempPrefixes;
        List<Future<List<File>>> futures = new ArrayList<>();
        for (File directory : directories) {
            if (atomicLong.get() > threads) {
                tempPrefixes = directoriesAfterListerRun(directory);
                if (tempPrefixes != null) listForNextIteratively(tempPrefixes);
            } else {
                atomicLong.incrementAndGet();
                futures.add(executorPool.submit(() -> {
                    List<File> list = directoriesAfterListerRun(directory);
                    atomicLong.decrementAndGet();
                    return list;
                }));
            }
        }
        Iterator<Future<List<File>>> iterator;
        Future<List<File>> future;
        while (futures.size() > 0) {
            iterator = futures.iterator();
            while (iterator.hasNext()) {
                future = iterator.next();
                if (future.isDone()) {
                    tempPrefixes = future.get();
                    if (tempPrefixes != null) listForNextIteratively(tempPrefixes);
                    iterator.remove();
                }
            }
        }
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    @Override
    public void export() throws Exception {
        String info = processor == null ?
                String.join(" ", "read objects from file(s):", path) :
                String.join(" ", "read objects from file(s):", path, "and", processor.getProcessName());
        rootLogger.info("{} running...", info);
        if (directories == null || directories.size() == 0) {
            FilepathLister filepathLister = new FilepathLister(new File(realPath), true, transferPath, leftTrimSize);
            if (filepathLister.getFileInfos().size() > 0) {
                reading(new FileInfoReader(path, filepathLister.getFileInfos(), filepathLister.getFileInfos().get(0), unitLen));
            }
            if (filepathLister.getDirectories() == null || filepathLister.getDirectories().size() <= 0) {
                rootLogger.info("{} finished.", info);
                return;
            } else if (hasAntiDirectories) {
                directories = filepathLister.getDirectories().parallelStream().filter(this::checkDirectory)
                        .peek(directory -> recordListerByDirectory(directory.getPath())).collect(Collectors.toList());
            } else {
                for (File dir : filepathLister.getDirectories()) recordListerByDirectory(dir.getPath());
                directories = filepathLister.getDirectories();
            }
        }
        executorPool = Executors.newFixedThreadPool(threads);
        showdownHook();
        try {
            listForNextIteratively(directories);
            executorPool.shutdown();
            while (!executorPool.isTerminated()) {
                sleep(1000);
            }
            rootLogger.info("{} finished.", info);
            endAction();
        } catch (Throwable e) {
            executorPool.shutdownNow();
            rootLogger.error(e.toString(), e);
            endAction();
            System.exit(-1);
        }
    }
}
