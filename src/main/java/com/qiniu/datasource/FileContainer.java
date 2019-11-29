package com.qiniu.datasource;

import com.google.gson.JsonObject;
import com.qiniu.common.QiniuException;
import com.qiniu.interfaces.*;
import com.qiniu.util.*;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public abstract class FileContainer<E, W, T> extends DatasourceActor implements IDataSource<ILocalFileLister<E, File>, IResultOutput<W>, T> {

    protected String path;
    protected boolean keepDir;
    protected String transferPath = null;
    protected int leftTrimSize = 0;
    protected String realPath;
    protected List<String> antiDirectories;
    protected boolean hasAntiDirectories = false;
    protected Map<String, Map<String, String>> directoriesMap;
    protected List<File> directories;
    protected ILineProcess<T> processor; // 定义的资源处理器
//    protected List<ILocalFileLister<E, File>> listerList = new ArrayList<>(threads);
    protected ConcurrentMap<String, ILocalFileLister<E, File>> listerMap = new ConcurrentHashMap<>(threads);

    public FileContainer(String path, Map<String, Map<String, String>> directoriesMap, List<String> antiDirectories, boolean keepDir,
                         Map<String, String> indexMap, List<String> fields, int unitLen, int threads) throws IOException {
        super(unitLen, threads);
        this.path = path;
        this.keepDir = keepDir;
        setAntiDirectories(antiDirectories);
        setTransferPathAndLeftTrimSize();
        setDirectoriesAndMap(directoriesMap);
        setIndexMapWithDefault(indexMap);
        if (fields != null && fields.size() > 0) this.fields = fields;
        else this.fields = ConvertingUtils.getOrderedFields(this.indexMap, null);
        // default save parameters，默认全记录保存
        setSaveOptions(true, "result", "tab", "\t", null);
    }

    private void setAntiDirectories(List<String> antiDirectories) {
        if (antiDirectories != null && antiDirectories.size() > 0) {
            hasAntiDirectories = true;
            this.antiDirectories = antiDirectories.stream().sorted().collect(Collectors.toList());
            int size = this.antiDirectories.size();
            Iterator<String> iterator = this.antiDirectories.iterator();
            String temp = iterator.next();
            while (iterator.hasNext() && size > 0) {
                size--;
                String prefix = iterator.next();
                if (prefix.startsWith(temp)) iterator.remove();
                else temp = prefix;
            }
        }
    }

    private void setTransferPathAndLeftTrimSize() throws IOException {
        if (path.indexOf(FileUtils.pathSeparator + FileUtils.currentPath) > 0 ||
                path.indexOf(FileUtils.pathSeparator + FileUtils.parentPath) > 0 ||
                path.endsWith(FileUtils.pathSeparator + ".") ||
                path.endsWith(FileUtils.pathSeparator + "..")) {
            throw new IOException("please set straight path, can not contain \"/..\" or \"/.\".");
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
        } else {
            if (directoriesMap.containsKey(null)) throw new IOException("can not find directory named \"null\".");
            this.directoriesMap = new HashMap<>(threads);
            this.directoriesMap.putAll(directoriesMap);
            directories = new ArrayList<>();
            int size = this.directoriesMap.size();
            Iterator<String> iterator = this.directoriesMap.keySet().stream()
                    .map(directory -> directory = directory.split("-\\|\\|-")[0])
                    .sorted().collect(Collectors.toList()).iterator();
            String temp = iterator.next();
            File tempFile;
            Map<String, String> value = directoriesMap.get(temp);
            String end;
            if (temp == null || temp.equals("")) {
                throw new IOException("directories can not contains empty item");
            } else {
                tempFile = new File(temp);
                if (!tempFile.exists()) tempFile = new File(realPath, temp);
                if (tempFile.isDirectory()) directories.add(tempFile);
            }
            while (iterator.hasNext() && size > 0) {
                size--;
                String directory = iterator.next();
                if (directory == null || directory.equals("")) {
                    throw new IOException("directories can not contains empty item");
                } else {
                    File file = new File(directory);
                    if (!file.exists()) file = new File(realPath, directory);
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

                    } else {
                        throw new IOException(directory + " is not valid directory.");
                    }
                }
            }
        }
    }

    private void setIndexMapWithDefault(Map<String, String> indexMap) throws IOException {
        if (indexMap == null || indexMap.size() == 0) {
            if (this.indexMap == null) this.indexMap = new HashMap<>();
            for (String fileInfoField : ConvertingUtils.defaultFileInfos) {
                this.indexMap.put(fileInfoField, fileInfoField);
            }
        } else {
            for (String s : indexMap.keySet()) {
                if (s == null || "".equals(s)) throw new IOException("the index can not be empty in " + indexMap);
            }
            this.indexMap = indexMap;
        }
    }

    public void setProcessor(ILineProcess<T> processor) {
        this.processor = processor;
    }

    protected abstract ITypeConvert<E, T> getNewConverter() throws IOException;

    protected abstract ITypeConvert<E, String> getNewStringConverter() throws IOException;

    private boolean checkDirectory(File directory) {
//        if (hasAntiDirectories) {
            for (String antiPrefix : antiDirectories) {
                if (directory.getPath().startsWith(antiPrefix)) return false;
            }
            return true;
//        } else {
//            return true;
//        }
    }

    protected abstract ILocalFileLister<E, File> getLister(File directory, String start, String end, int unitLen) throws IOException;

    protected abstract ILocalFileLister<E, File> getLister(String name, List<E> fileInfoList, String start,
                                                           String end, int unitLen) throws IOException;

    protected abstract ILocalFileLister<E, File> getLister(String singleFilePath) throws IOException;

    private ILocalFileLister<E, File> generateLister(File directory) throws IOException {
        Map<String, String> map = directoriesMap.get(directory.getPath());
        String start;
        String end;
        if (map == null) {
            start = end = null;
        } else {
            start = map.get("start");
            end = map.get("end");
        }
        return getLister(directory, start, end, unitLen);
    }

    public void export(ILocalFileLister<E, File> lister, IResultOutput<W> saver, ILineProcess<T> processor) throws Exception {
        ITypeConvert<E, T> converter = getNewConverter();
        ITypeConvert<E, String> stringConverter = null;
        if (saveTotal) {
            stringConverter = getNewStringConverter();
            saver.preAddWriter("failed");
        }
        List<T> convertedList;
        List<String> writeList;
        List<E> objects = lister.currents();
        boolean hasNext = lister.hasNext();
        // 初始化的 lister 包含首次列举的结果列表，需要先取出，后续向前列举时会更新其结果列表
        while (objects.size() > 0 || hasNext) {
            if (stopped) break;
            if (LocalDateTime.now(DatetimeUtils.clock_Default).isAfter(pauseDateTime)) {
                synchronized (object) {
                    object.wait();
                }
            }
            if (stringConverter != null) {
                writeList = stringConverter.convertToVList(objects);
                if (writeList.size() > 0) saver.writeSuccess(String.join("\n", writeList), false);
                if (stringConverter.errorSize() > 0) saver.writeToKey("failed", stringConverter.errorLines(), false);
            }
            if (processor != null) {
                convertedList = converter.convertToVList(objects);
                if (converter.errorSize() > 0) saver.writeError(converter.errorLines(), false);
                // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
                try {
                    processor.processLine(convertedList);
                } catch (QiniuException e) {
                    if (HttpRespUtils.checkException(e, 2) < -1) throw e;
                    errorLogger.error("process objects: {}", lister.getName(), e);
                    if (e.response != null) e.response.close();
                }
            }
            if (hasNext) {
                JsonObject json = recorder.getOrDefault(lister.getName(), new JsonObject());
                json.addProperty("end", lister.getEndPrefix());
                try { FileUtils.createIfNotExists(procedureLogFile); } catch (IOException ignored) {}
                procedureLogger.info(recorder.put(lister.getName(), json));
            }
            if (stopped) break;
//            objects.clear(); 上次其实不能做 clear，会导致 lister 中的列表被清空
            lister.listForward();
            objects = lister.currents();
            hasNext = lister.hasNext();
        }
    }

    protected abstract IResultOutput<W> getNewResultSaver(String order) throws IOException;

    private void listing(ILocalFileLister<E, File> lister) {
        int order = UniOrderUtils.getOrder();
        String orderStr = String.valueOf(order);
        ILineProcess<T> lineProcessor = null;
        IResultOutput<W> saver = null;
        try {
            saver = getNewResultSaver(orderStr);
            saverMap.put(orderStr, saver);
            if (processor != null) {
                lineProcessor = processor.clone();
                processorMap.put(orderStr, lineProcessor);
            }
            export(lister, saver, lineProcessor);
            recorder.remove(lister.getName());
        }  catch (QiniuException e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}, {}", lister.getName(), recorder.getString(lister.getName()), e.error(), e);
            if (e.response != null) e.response.close();
        } catch (Throwable e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}", lister.getName(), recorder.getString(lister.getName()), e);
        } finally {
            try { FileUtils.createIfNotExists(infoLogFile); } catch (IOException ignored) {}
            infoLogger.info("{}\t{}\t{}", orderStr, lister.getName(), lister.count());
            if (saver != null) {
                saver.closeWriters();
                saver = null; // let gc work
            }
            saverMap.remove(orderStr);
            if (lineProcessor != null) {
                lineProcessor.closeResource();
                lineProcessor = null;
            }
            UniOrderUtils.returnOrder(order);
            lister.close();
            listerMap.remove(lister.getName());
        }
    }

    private void recordListerByDirectory(String name) {
        String pName = name.split("-\\|\\|-")[0];
        JsonObject json = directoriesMap.get(pName) == null ? null : JsonUtils.toJsonObject(directoriesMap.get(pName));
        try { FileUtils.createIfNotExists(procedureLogFile); } catch (IOException ignored) {}
        procedureLogger.info(recorder.put(name, json));
    }

    private void processNodeLister(ILocalFileLister<E, File> lister) {
        if (lister.hasNext()) {
            listerMap.put(lister.getName(), lister);
            executorPool.execute(() -> listing(lister));
        } else {
            recorder.remove(lister.getName());
            lister.close();
        }
    }

    private List<File> directoriesFromLister(File directory) {
        try {
            ILocalFileLister<E, File> lister = generateLister(directory);
            processNodeLister(lister);
            if (lister.getDirectories() == null || lister.getDirectories().size() <= 0) {
                return null;
            } else if (hasAntiDirectories) {
                return lister.getDirectories().stream().filter(this::checkDirectory)
                        .peek(dir -> recordListerByDirectory(dir.getPath())).collect(Collectors.toList());
            } else {
                for (File dir : lister.getDirectories()) recordListerByDirectory(dir.getPath());
                return lister.getDirectories();
            }
        } catch (IOException e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("generate lister failed by {}\t{}", directory.getPath(), directoriesMap.get(directory.getPath()), e);
            return null;
        }
    }

    private List<ILocalFileLister<E, File>> checkListerInPool(int cValue, int tiny) {
        int count = 0;
        ILocalFileLister<E, File> iLister;
        boolean notCheck = true;
        List<ILocalFileLister<E, File>> list = new ArrayList<>(listerMap.values());
        Iterator<ILocalFileLister<E, File>> iterator = list.iterator();
        String directory;
        String start;
        Map<String, String> endMap;
        while (!executorPool.isTerminated()) {
            if (count >= 1200) {
                notCheck = false;
                while (iterator.hasNext()) {
                    iLister = iterator.next();
                    if(!iLister.hasNext()) iterator.remove();
                }
                if (list.size() > 0 && list.size() <= tiny) {
                    rootLogger.info("unfinished: {}, cValue: {}, to re-split lister list...\n", list.size(), cValue);
                    for (ILocalFileLister<E, File> lister : list) {
                        // lister 的 prefix 为 final 对象，不能因为 truncate 的操作之后被修改
                        directory = lister.getName();
                        start = lister.truncate();
                        endMap = directoriesMap.get(directory);
                        if (endMap == null) endMap = new HashMap<>();
                        endMap.put("start", start);
                        rootLogger.info("directory: {}, nextFilepath: {}, endMap: {}\n", directory, start, endMap);
                    }
                } else if (list.size() <= cValue) {
                    count = 900;
                } else {
                    count = 0;
                }
            }
            sleep(1000);
            count++;
        }
        if (notCheck) return new ArrayList<>();
        else return list;
    }

    private void directoriesListing() {
        while (directories != null && directories.size() > 0) {
            directories = directories.parallelStream().map(this::directoriesFromLister).filter(Objects::nonNull)
                    .reduce((list1, list2) -> { list1.addAll(list2); return list1; }).orElse(null);
        }
        executorPool.shutdown();
        if (threads > 1) {
            int cValue = threads >= 10 ? threads / 2 : 3;
            int tiny = threads >= 300 ? 30 : threads >= 200 ? 20 : threads >= 100 ? 10 : threads >= 30 ? threads / 10 :
                    threads >= 10 ? 3 : 1;
            List<ILocalFileLister<E, File>> list = checkListerInPool(cValue, tiny);
            while (list.size() > 0) {
                list.parallelStream().forEach(lister -> recordListerByDirectory(lister.getName() + "-||-0"));
                int multiple = threads / list.size();
                int maxIndex = multiple - 1;
                executorPool = Executors.newFixedThreadPool(threads);
                listerMap.clear();
                list.parallelStream().forEach(lister -> {
                    if (lister.getRemainedFiles() == null) return;
                    int remainedSize = lister.getRemainedFiles().size();
                    if (remainedSize < multiple) {
                        if (remainedSize > 0) {
                            try {
                                ILocalFileLister<E, File> sLister = getLister(lister.getName() + "-||-0",
                                        lister.getRemainedFiles(), null, null, unitLen);
                                listerMap.put(sLister.getName(), sLister);
                                executorPool.execute(() -> listing(lister));
                            } catch (IOException e) {
                                try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                                errorLogger.error("generate lister failed by {}\t{}", lister.getName(),
                                        directoriesMap.get(lister.getName()), e);
                            }
                        }
                        return;
                    }
                    int size = remainedSize % multiple == 0 ? remainedSize / multiple : remainedSize / multiple + 1;
                    for (int i = 0; i < multiple; i++) {
                        try {
                            ILocalFileLister<E, File> sLister = getLister(String.join("-||-", lister.getName(), String.valueOf(i)),
                                    lister.getRemainedFiles().subList(size * i, i == maxIndex ? remainedSize : size * (i + 1)),
                                    null, null, unitLen);
                            listerMap.put(sLister.getName(), sLister);
                            executorPool.execute(() -> listing(lister));
                        } catch (IOException e) {
                            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                            errorLogger.error("generate lister failed by {}\t{}",
                                    String.join("-||-", lister.getName(), String.valueOf(i)),
                                    directoriesMap.get(lister.getName()), e);
                        }
                    }
                });
                executorPool.shutdown();
                list = checkListerInPool(cValue, tiny);
            }
        }
        while (!executorPool.isTerminated()) sleep(1000);
    }

    @Override
    public void export() throws Exception {
        String info = processor == null ? String.join(" ", "list files from path:", path) :
                String.join(" ", "read files from path: ", path, "and", processor.getProcessName());
        rootLogger.info("{} running...", info);
        rootLogger.info("order\tprefix\tquantity");
        showdownHook();
        ILocalFileLister<E, File> fileInfoLister = null;
        if (directories == null || directories.size() == 0) {
            File file = new File(realPath);
            if (file.isDirectory()) {
                recordListerByDirectory(realPath);
                fileInfoLister = generateLister(file);
            } else {
                fileInfoLister = getLister(realPath);
            }
            directories = fileInfoLister.getDirectories();
        }
        try {
            if (directories == null || directories.size() == 0) {
                if (fileInfoLister.hasNext()) {
                    listing(fileInfoLister);
                } else {
                    recorder.remove(fileInfoLister.getName());
                    fileInfoLister.close();
                }
            } else {
                if (hasAntiDirectories) {
                    directories = directories.parallelStream().filter(this::checkDirectory)
                            .peek(directory -> recordListerByDirectory(directory.getPath())).collect(Collectors.toList());
                } else {
                    directories.parallelStream().forEach(directory -> recordListerByDirectory(directory.getPath()));
                }
                executorPool = Executors.newFixedThreadPool(threads);
                if (fileInfoLister != null) processNodeLister(fileInfoLister);
                directoriesListing();
            }
            rootLogger.info("{} finished, results in {}.", info, savePath);
            endAction();
        } catch (Throwable e) {
            stopped = true;
            rootLogger.error(e.toString(), e);
            endAction();
            System.exit(-1);
        }
    }
}
