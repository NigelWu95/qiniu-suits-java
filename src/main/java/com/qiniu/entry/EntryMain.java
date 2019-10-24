package com.qiniu.entry;

import com.qiniu.config.ParamsConfig;
import com.qiniu.config.PropertiesFile;
import com.qiniu.interfaces.IDataSource;
import com.qiniu.datasource.InputSource;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.util.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class EntryMain {

    // 如果不希望对危险 process 进行 verify，请将该参数设置为 false
    public static boolean processVerify = true;

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, String> preSetMap = new HashMap<String, String>(){{
            put("f", "verify=false");
            put("s", "single=true");
            put("single", "single=true");
            put("line", "single=true");
            put("i", "interactive=true");
            put("interactive", "interactive=true");
            put("d", "default=true"); // for default account setting
            put("getaccount", "getaccount=default"); // for default account query
            put("dis", "displayed=false"); // get default account without displayed secret
        }};
        Map<String, String> paramsMap = getEntryParams(args, preSetMap);
        IEntryParam entryParam = new ParamsConfig(paramsMap);
        if (paramsMap.containsKey("account")) {
            AccountUtils.setAccount(entryParam, paramsMap.get("account"));
            return;
        } else if (paramsMap.containsKey("getaccount")) {
            Boolean implicit = !paramsMap.containsKey("displayed");
            String accountName = paramsMap.get("getaccount");
            if ("default".equals(accountName)) {
                if (entryParam.getValue("default", "false").equals("true")) accountName = null;
                else throw new IOException("please set account name or use \"-d\"");
            }
            List<String[]> keysList = AccountUtils.getAccount(accountName, implicit);
            for (String[] keys : keysList) {
                System.out.println(keys[2] + ": ");
                System.out.println("id: " + keys[0]);
                System.out.println("secret: " + keys[1]);
            }
            return;
        } else if (paramsMap.containsKey("delaccount")) {
            AccountUtils.deleteAccount(paramsMap.get("delaccount"));
            return;
        }
        if (paramsMap.containsKey("verify")) processVerify = Boolean.parseBoolean(paramsMap.get("verify"));
        boolean single = paramsMap.containsKey("single") && Boolean.parseBoolean(paramsMap.get("single"));
        boolean interactive = paramsMap.containsKey("interactive") && Boolean.parseBoolean(paramsMap.get("interactive"));
        CommonParams commonParams = single ? new CommonParams(paramsMap) : new CommonParams(entryParam);
        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam, commonParams);
        ILineProcess<Map<String, String>> processor = single || interactive ? qSuitsEntry.whichNextProcessor(true) :
                qSuitsEntry.getProcessor();
        if (processVerify && processor != null) {
            String process = processor.getProcessName();
            if (processor.getNextProcessor() != null) process = processor.getNextProcessor().getProcessName();
            if (ProcessUtils.isDangerous(process)) {
                System.out.println("your last process is " + process + ", are you sure? (y/n): ");
                Scanner scanner = new Scanner(System.in);
                String an = scanner.next();
                if (!an.equalsIgnoreCase("y") && !an.equalsIgnoreCase("yes")) {
                    return;
                }
            }
        }
        if (single) {
            if (processor != null) {
                Map<String, String> converted = commonParams.getMapLine();
                if ("qupload".equals(processor.getProcessName())) {
                    if (converted.containsKey("filepath")) {
                        converted.put("filepath", FileUtils.convertToRealPath(converted.get("filepath")));
                    } else {
                        converted.put("filepath", FileUtils.convertToRealPath(converted.get("key")));
                    }
                }
                System.out.println(processor.processLine(converted));
            }
        } else if (interactive) {
            InputSource inputSource = qSuitsEntry.getInputSource();
            inputSource.export(System.in, processor);
        } else {
            IDataSource dataSource = qSuitsEntry.getDataSource();
            if (dataSource != null) {
                dataSource.setProcessor(processor);
//                dataSource.export();
                dataSource.export(commonParams.getStartDateTime(), commonParams.getPauseDelay(), commonParams.getPauseDuration());
//                dataSource.export(LocalDateTime.now().plusSeconds(5), 0, 19);
            }
        }
        if (processor != null) processor.closeResource();
    }

    public static Map<String, String> getEntryParams(String[] args, Map<String, String> preSetMap) throws IOException {
        Map<String, String> paramsMap = args != null && args.length > 0 ? ParamsUtils.toParamsMap(args, preSetMap) : null;
        if (paramsMap != null && paramsMap.containsKey("config")) {
            Map<String, String> fileConfig = ParamsUtils.toParamsMap(paramsMap.get("config"));
            fileConfig.putAll(paramsMap);
            return fileConfig;
        } else {
            String configFilePath = null;
            List<String> configFiles = new ArrayList<String>(){{
                add("resources" + System.getProperty("file.separator") + "application.config");
                add("resources" + System.getProperty("file.separator") + "application.properties");
                add("resources" + System.getProperty("file.separator") + ".application.config");
                add("resources" + System.getProperty("file.separator") + ".application.properties");
            }};
            for (int i = configFiles.size() - 1; i >= 0; i--) {
                File file = new File(configFiles.get(i));
                if (file.exists()) {
                    configFilePath = configFiles.get(i);
                    System.out.printf("use default config file: %s\n", configFilePath);
                    break;
                }
            }
            Map<String, String> fileConfig;
            if (configFilePath == null) {
                if (paramsMap == null) throw new IOException("there is no config file detected.");
                else return paramsMap;
            } else if (configFilePath.endsWith(".properties")) {
                fileConfig = ParamsUtils.toParamsMap(new PropertiesFile(configFilePath).getProperties());
            } else {
                fileConfig = ParamsUtils.toParamsMap(configFilePath);
            }
            if (paramsMap == null) {
                return fileConfig;
            } else {
                fileConfig.putAll(paramsMap);
                return fileConfig;
            }
        }
//        if (args != null && args.length > 0) {
//            Map<String, String> paramsMap = ParamsUtils.toParamsMap(args, preSetMap);
//            if (paramsMap.containsKey("config")) {
//                return ParamsUtils.toParamsMap(paramsMap.get("config"));
//            } else {
//                return paramsMap;
//            }
//        } else {
//            String configFilePath = null;
//            List<String> configFiles = new ArrayList<String>(){{
//                add("resources" + System.getProperty("file.separator") + "application.config");
//                add("resources" + System.getProperty("file.separator") + ".application.config");
//                add("resources" + System.getProperty("file.separator") + ".application.properties");
//            }};
//            for (int i = configFiles.size() - 1; i >= 0; i--) {
//                File file = new File(configFiles.get(i));
//                if (file.exists()) {
//                    configFilePath = configFiles.get(i);
//                    System.out.printf("use default config file: %s\n", configFilePath);
//                    break;
//                }
//            }
//            if (configFilePath == null) {
//                throw new IOException("there is no config file detected.");
//            } else if (configFilePath.endsWith(".properties")) {
//                return ParamsUtils.toParamsMap(new PropertiesFile(configFilePath).getProperties());
//            } else {
//                return ParamsUtils.toParamsMap(configFilePath);
//            }
//        }
    }
}
