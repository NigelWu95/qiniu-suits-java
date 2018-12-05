package com.qiniu.custom.miaop;

import com.qiniu.common.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.model.qoss.Qhash;
import com.qiniu.service.convert.QhashToString;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;
import com.qiniu.service.qoss.FileChecker;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QhashProcess implements ILineProcess<Map<String, String>>, Cloneable {

    private String domain;
    private boolean https;
    private Auth srcAuth;
    private FileChecker fileChecker;
    private String processName;
    private int retryCount = 3;
    protected String resultFileDir;
    private FileMap fileMap;
    private ITypeConvert<Qhash, String> typeConverter;

    private void initBaseParams(String domain) {
        this.processName = "hash";
        this.domain = domain;
    }

    public QhashProcess(String domain, String resultFileDir) {
        initBaseParams(domain);
        this.resultFileDir = resultFileDir;
        this.fileChecker = new FileChecker("md5", https, srcAuth);
        this.fileMap = new FileMap();
    }

    public QhashProcess(String domain, String resultFileDir, int resultFileIndex) throws IOException {
        this(domain, resultFileDir);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public void setOptions(boolean https, Auth srcAuth) {
        this.https = https;
        this.srcAuth = srcAuth;
    }

    public void setTypeConverter(String format, String separator) {
        this.typeConverter = new QhashToString(format, separator);
    }

    public QhashProcess getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        QhashProcess queryHash = (QhashProcess)super.clone();
        queryHash.fileChecker = new FileChecker(null, https, srcAuth);
        queryHash.fileMap = new FileMap();
        try {
            queryHash.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryHash;
    }

    public void setBatch(boolean batch) {}

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getProcessName() {
        return this.processName;
    }

    public String getInfo() {
        return domain;
    }

    public void processLine(List<Map<String, String>> lineList) throws QiniuException {

        List<String> successList = new ArrayList<>();
        List<String> failList = new ArrayList<>();
        for (Map<String, String> line : lineList) {
            try {
                Qhash qhash = null;
                try {
                    qhash = fileChecker.getQHash(domain, line.get("0"));
                } catch (QiniuException e1) {
                    HttpResponseUtils.checkRetryCount(e1, retryCount);
                    while (retryCount > 0) {
                        try {
                            qhash = fileChecker.getQHash(domain, line.get("0"));
                            retryCount = 0;
                        } catch (QiniuException e2) {
                            retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                        }
                    }
                }
                if (qhash == null) throw new QiniuException(null, "empty qhash");
                String md5 = qhash.hash;
                if (md5.equals(line.get("1"))) successList.add(line.get("0") + "\t" + typeConverter.toV(qhash));
                else failList.add(line.get("1") + "\t" + typeConverter.toV(qhash));
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, processName, getInfo() + "\t" + line.get("0"));
            }
        }
        if (successList.size() > 0) fileMap.writeSuccess(String.join("\n", successList));
        if (failList.size() > 0) fileMap.writeErrorOrNull(String.join("\n", failList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
