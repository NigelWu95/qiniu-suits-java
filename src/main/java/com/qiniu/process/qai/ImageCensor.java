package com.qiniu.process.qai;

import com.qiniu.common.QiniuException;
import com.qiniu.process.Base;
import com.qiniu.process.qdora.MediaManager;
import com.qiniu.storage.Configuration;

import java.io.IOException;
import java.util.Map;

public class ImageCensor extends Base<Map<String, String>> {

    private String protocol;
    private String pidIndex;
    private Configuration configuration;
    private MediaManager mediaManager;

    public ImageCensor(Configuration configuration, String protocol, String persistentIdIndex) throws IOException {
        super("pfopresult", "", "", null);
        set(configuration, protocol, persistentIdIndex);
        this.mediaManager = new MediaManager(configuration.clone(), protocol);
        this.fileSaveMapper.preAddWriter("waiting");
        this.fileSaveMapper.preAddWriter("notify_failed");
    }

    public ImageCensor(Configuration configuration, String protocol, String persistentIdIndex, String savePath,
                       int saveIndex) throws IOException {
        super("pfopresult", "", "", null, savePath, saveIndex);
        set(configuration, protocol, persistentIdIndex);
        this.mediaManager = new MediaManager(configuration.clone(), protocol);
        this.fileSaveMapper.preAddWriter("waiting");
        this.fileSaveMapper.preAddWriter("notify_failed");
    }

    public ImageCensor(Configuration configuration, String protocol, String persistentIdIndex, String savePath)
            throws IOException {
        this(configuration, protocol, persistentIdIndex, savePath, 0);
    }

    private void set(Configuration configuration, String protocol, String pidIndex) throws IOException {
        this.configuration = configuration;
        this.protocol = protocol;
        if (pidIndex == null || "".equals(pidIndex)) throw new IOException("please set the persistentId-index.");
        else this.pidIndex = pidIndex;
    }

    public void updateSavePath(String savePath) throws IOException {
        super.updateSavePath(savePath);
        this.fileSaveMapper.preAddWriter("waiting");
        this.fileSaveMapper.preAddWriter("notify_failed");
    }

    public void updateProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void updatePidIndex(String pidIndex) {
        this.pidIndex = pidIndex;
    }

    public ImageCensor clone() throws CloneNotSupportedException {
        ImageCensor pfopResult = (ImageCensor)super.clone();
        pfopResult.mediaManager = new MediaManager(configuration.clone(), protocol);
        pfopResult.fileSaveMapper.preAddWriter("waiting");
        pfopResult.fileSaveMapper.preAddWriter("notify_failed");
        return pfopResult;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get(pidIndex);
    }

    @Override
    protected String singleResult(Map<String, String> line) throws QiniuException {
        String pid = line.get(pidIndex);
        return pid + "\t" + mediaManager.getPfopResultBodyById(pid);
    }

    @Override
    public void closeResource() {
        super.closeResource();
        configuration = null;
        mediaManager = null;
        protocol = null;
        pidIndex = null;
    }
}
