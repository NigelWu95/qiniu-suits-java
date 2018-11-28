package com.qiniu.custom.fantx;

import com.qiniu.common.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.model.media.PfopResult;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PfopResultProcess implements ILineProcess<Map<String, String>>, Cloneable {

    private String processName;
    protected String resultFileDir;
    private FileMap fileMap;

    private void initBaseParams() {
        this.processName = "fopresult";
    }

    public PfopResultProcess(String resultFileDir) {
        initBaseParams();
        this.resultFileDir = resultFileDir;
        this.fileMap = new FileMap();
    }

    public PfopResultProcess(String resultFileDir, int resultFileIndex) throws IOException {
        this(resultFileDir);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public PfopResultProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        PfopResultProcess queryPfopResult = (PfopResultProcess)super.clone();
        queryPfopResult.fileMap = new FileMap();
        try {
            queryPfopResult.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryPfopResult;
    }

    public void setBatch(boolean batch) {}

    public void setRetryCount(int retryCount) {}

    public String getProcessName() {
        return this.processName;
    }

    public String getInfo() {
        return "";
    }

    public void processLine(List<Map<String, String>> lineList) throws QiniuException {

        lineList = lineList == null ? null : lineList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (lineList == null || lineList.size() == 0) return;
        List<String> successList = new ArrayList<>();
        List<String> failList = new ArrayList<>();
        for (Map<String, String> line : lineList) {
            try {
                PfopResult pfopResult = JsonConvertUtils.fromJson(line.get("0"), PfopResult.class);
                if (pfopResult == null) throw new QiniuException(null, "empty pfop result");
                if (pfopResult.code == 0) successList.add(pfopResult.inputKey + "\t" + pfopResult.items.get(0).key);
                else failList.add(pfopResult.inputKey + "\t" + pfopResult.id + "\t" + pfopResult.code + "\t" + pfopResult.desc);
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, processName, getInfo() + "\t" + line.toString());
            }
        }
        if (successList.size() > 0) fileMap.writeSuccess(String.join("\n", successList));
        if (failList.size() > 0) fileMap.writeErrorOrNull(String.join("\n", failList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
