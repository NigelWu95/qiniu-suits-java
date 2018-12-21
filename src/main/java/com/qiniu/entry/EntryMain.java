package com.qiniu.entry;

import com.qiniu.config.CommandArgs;
import com.qiniu.config.PropertyConfig;
import com.qiniu.service.interfaces.IEntryParam;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class EntryMain {

    public static void main(String[] args) throws Exception {

        List<String> configFiles = new ArrayList<String>(){{
            add("resources/qiniu.properties");
            add("resources/.qiniu.properties");
        }};
        boolean paramFromConfig = true;
        if (args != null && args.length > 0) {
            if (args[0].startsWith("-config=")) configFiles.add(args[0].split("=")[1]);
            else paramFromConfig = false;
        }
        String configFilePath = null;
        if (paramFromConfig) {
            for (int i = configFiles.size() - 1; i >= 0; i--) {
                File file = new File(configFiles.get(i));
                if (file.exists()) {
                    configFilePath = configFiles.get(i);
                    break;
                }
            }
            if (configFilePath == null) throw new Exception("there is no config file detected.");
            else paramFromConfig = true;
        }

        IEntryParam entryParam = paramFromConfig ? new PropertyConfig(configFilePath) : new CommandArgs(args);
        String sourceType = entryParam.getParamValue("source-type");

        if ("list".equals(sourceType)) ListBucketEntry.run(entryParam);
        else if ("file".equals(sourceType)) FileInputEntry.run(entryParam);
    }
}
