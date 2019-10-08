package com.qiniu.entry;

import com.qiniu.config.ParamsConfig;
import com.qiniu.config.PropertiesFile;
import com.qiniu.interfaces.IDataSource;
import com.qiniu.datasource.InputSource;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.util.EncryptUtils;
import com.qiniu.util.FileUtils;
import com.qiniu.util.ParamsUtils;
import com.qiniu.util.ProcessUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class EntryMain {

    public static boolean process_verify = true;

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
        }};
        Map<String, String> paramsMap = getEntryParams(args, preSetMap);
        IEntryParam entryParam = new ParamsConfig(paramsMap);
        if (paramsMap.containsKey("account")) {
            setAccount(entryParam, paramsMap.get("account"));
            return;
        }
        if (paramsMap.containsKey("verify")) process_verify = Boolean.parseBoolean(paramsMap.get("verify"));
        boolean single = paramsMap.containsKey("single") && Boolean.parseBoolean(paramsMap.get("single"));
        boolean interactive = paramsMap.containsKey("interactive") && Boolean.parseBoolean(paramsMap.get("interactive"));
        CommonParams commonParams = single ? new CommonParams(paramsMap) : new CommonParams(entryParam);
        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam, commonParams);
        ILineProcess<Map<String, String>> processor = single || interactive ? qSuitsEntry.whichNextProcessor(true) :
                qSuitsEntry.getProcessor();
        if (process_verify && processor != null) {
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

    private static void setAccount(IEntryParam entryParam, String account) throws Exception {
        String filePath = FileUtils.convertToRealPath("~" + FileUtils.pathSeparator + ".qsuits.account");
        File accountFile = new File(filePath);
        boolean accountFileExists = (!accountFile.isDirectory() && accountFile.exists()) || accountFile.createNewFile();
        if (!accountFileExists) throw new IOException("account file not exists and can not be created.");
        String id;
        String secret;
        Base64.Encoder encoder = Base64.getEncoder();
        if (account == null) {
            throw new IOException("account name is empty.");
        } else if (account.startsWith("ten-")) {
            account = account.substring(4);
            id = account + "-tencent-id=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("ten-id").getBytes()));
            secret = account + "-tencent-secret=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("ten-secret").getBytes()));
        } else if (account.startsWith("ali-")) {
            account = account.substring(4);
            id = account + "-aliyun-id=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("ali-id").getBytes()));
            secret = account + "-aliyun-secret=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("ali-secret").getBytes()));
        } else if (account.startsWith("up-")) {
            account = account.substring(3);
            id = account + "-upyun-id=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("up-id").getBytes()));
            secret = account + "-upyun-secret=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("up-secret").getBytes()));
        } else if (account.startsWith("s3-") || account.startsWith("aws-")) {
            account = account.substring(3);
            id = account + "-s3-id=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("s3-id").getBytes()));
            secret = account + "-s3-secret=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("s3-secret").getBytes()));
        } else if (account.startsWith("hua-")) {
            account = account.substring(4);
            id = account + "-huawei-id=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("hua-id").getBytes()));
            secret = account + "-huawei-secret=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("hua-secret").getBytes()));
        } else if (account.startsWith("bai-")) {
            account = account.substring(4);
            id = account + "-baidu-id=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("bai-id").getBytes()));
            secret = account + "-baidu-secret=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("bai-secret").getBytes()));
        } else {
            if (account.startsWith("qiniu-")) account = account.substring(6);
            id = account + "-qiniu-id=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("ak").getBytes()));
            secret = account + "-qiniu-secret=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("sk").getBytes()));
        }
        int idIndex = id.indexOf("=");
        int secretIndex = secret.indexOf("=");
        Map<String, String> map = ParamsUtils.toParamsMap(filePath);
        String valueId = map.get(id.substring(0, idIndex));
        String valueSecret = map.get(secret.substring(0, secretIndex));
        if (entryParam.getValue("default", "false").equals("true")) {
            map.put("account", account);
            String oldAccount = map.get("account");
            if (oldAccount == null) {
                BufferedWriter writer = new BufferedWriter(new FileWriter(accountFile, true));
                writer.write("account=" + account);
                writer.newLine();
                writer.close();
            } else {
                BufferedWriter writer = new BufferedWriter(new FileWriter(accountFile));
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    writer.write(entry.getKey() + "=" + entry.getValue());
                    writer.newLine();
                }
                writer.close();
//                FileUtils.randomModify(filePath, "account=" + oldAccount,
//                        "account=" + encoder.encode(account.getBytes()));
            }
        }
        if (valueId != null || valueSecret != null) {
            map.put(id.substring(0, idIndex), id.substring(idIndex + 1));
            map.put(secret.substring(0, secretIndex), secret.substring(secretIndex + 1));
            BufferedWriter writer = new BufferedWriter(new FileWriter(accountFile));
            for (Map.Entry<String, String> entry : map.entrySet()) {
                writer.write(entry.getKey() + "=" + entry.getValue());
                writer.newLine();
            }
            writer.close();
//            FileUtils.randomModify(filePath, keyId + "=" + valueId, id);
//            FileUtils.randomModify(filePath, keySecret + "=" + valueSecret, secret);
        } else {
            BufferedWriter writer = new BufferedWriter(new FileWriter(accountFile, true));
            writer.write(id);
            writer.newLine();
            writer.write(secret);
            writer.newLine();
            writer.close();
        }
    }
}
