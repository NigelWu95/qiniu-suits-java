package com.qiniu.util;

import com.qiniu.interfaces.IEntryParam;

import java.io.*;
import java.util.*;
import java.util.Base64;

public class AccountUtils {

    public static String accountPath = String.join(FileUtils.pathSeparator, "~", ".qsuits.account");
    private static Base64.Encoder encoder = Base64.getEncoder();
    private static Base64.Decoder decoder = Base64.getDecoder();

    public static void setAccount(IEntryParam entryParam, String account) throws Exception {
        String filePath = FileUtils.convertToRealPath("~" + FileUtils.pathSeparator + ".qsuits.account");
        File accountFile = new File(filePath);
        boolean accountFileExists = (!accountFile.isDirectory() && accountFile.exists()) || accountFile.createNewFile();
        if (!accountFileExists) throw new IOException("account file not exists and can not be created.");
        String id;
        String secret;
        String accountName;
        if (account == null) {
            throw new IOException("account name is empty.");
        } else if (account.startsWith("qiniu-")) {
            accountName = account.substring(6);
            id = String.join("-", accountName, CloudApiUtils.QINIU, "id=") +
                    EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("ak").trim().getBytes()));
            secret = String.join("-", accountName, CloudApiUtils.QINIU, "secret=") +
                    EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("sk").trim().getBytes()));
        } else if (account.contains("-")) {
            String sour = account.substring(0, account.indexOf("-"));
            String source;
            switch (sour) {
                case "ten": source = CloudApiUtils.TENCENT; break;
                case "ali": source = CloudApiUtils.ALIYUN; break;
                case "up": source = CloudApiUtils.UPYUN; break;
                case "aws": sour = CloudApiUtils.AWSS3;
                case "s3": source = CloudApiUtils.AWSS3; break;
                case "bai": source = CloudApiUtils.BAIDU; break;
                case "hua": source = CloudApiUtils.HUAWEI; break;
                default: throw new IOException("no such datasource to set account: " + sour);
            }
            id = entryParam.getValue(String.join("-", sour, "id")).trim();
            id = new String(encoder.encode(id.getBytes()));
            secret = entryParam.getValue(String.join("-", sour, "secret")).trim();
            secret = new String(encoder.encode(secret.getBytes()));
            accountName = account.substring(account.indexOf("-") + 1);
            id = String.join("-", accountName, source, "id=") + EncryptUtils.getRandomString(8) + id;
            secret = String.join("-", accountName, source, "secret=") + EncryptUtils.getRandomString(8) + secret;
        } else {
            accountName = account;
            id = String.join("-", account, CloudApiUtils.QINIU, "id=") +
                    EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("ak").trim().getBytes()));
            secret = String.join("-", account, CloudApiUtils.QINIU, "secret=") +
                    EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("sk").trim().getBytes()));
        }
        int idIndex = id.indexOf("=");
        int secretIndex = secret.indexOf("=");
        Map<String, String> map = ParamsUtils.toParamsMap(filePath);
        String valueId = map.get(id.substring(0, idIndex));
        String valueSecret = map.get(secret.substring(0, secretIndex));
        if (entryParam.getValue("default", "false").equals("true")) {
            map.put("account", accountName);
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
    public static void deleteAccount(String account) throws Exception {
        String filePath = FileUtils.convertToRealPath("~" + FileUtils.pathSeparator + ".qsuits.account");
        File accountFile = new File(filePath);
        boolean accountFileExists = (!accountFile.isDirectory() && accountFile.exists()) || accountFile.createNewFile();
        if (!accountFileExists) throw new IOException("account file not exists and can not be created.");
        String id;
        String secret;
        String accountName;
        if (account == null) {
            throw new IOException("account name is empty.");
        } else if (account.contains("-")) {
            String sour = account.substring(0, account.indexOf("-"));
            String source;
            switch (sour) {
                case "qiniu": source = CloudApiUtils.QINIU; break;
                case "ten": source = CloudApiUtils.TENCENT; break;
                case "ali": source = CloudApiUtils.ALIYUN; break;
                case "up": source = CloudApiUtils.UPYUN; break;
                case "aws":
                case "s3": source = CloudApiUtils.AWSS3; break;
                case "bai": source = CloudApiUtils.BAIDU; break;
                case "hua": source = CloudApiUtils.HUAWEI; break;
                default: throw new IOException("no such datasource to set account: " + sour);
            }
            accountName = account.substring(account.indexOf("-") + 1);
            id = String.join("-", accountName, source, "id");
            secret = String.join("-", accountName, source, "secret");
        } else {
            id = String.join("-", account, CloudApiUtils.QINIU, "id");
            secret = String.join("-", account, CloudApiUtils.QINIU, "secret");
        }
        Map<String, String> map = ParamsUtils.toParamsMap(filePath);
        String removedId = map.remove(id);
        String removedSecret = map.remove(secret);
        if (removedId == null && removedSecret == null) return;
        BufferedWriter writer = new BufferedWriter(new FileWriter(accountFile));
        for (Map.Entry<String, String> entry : map.entrySet()) {
            writer.write(entry.getKey() + "=" + entry.getValue());
            writer.newLine();
        }
        writer.close();
    }

    public static List<String[]> getAccount(String accountName, boolean secretMode) throws IOException {
        Map<String, String> accountMap;
        try {
            accountMap = ParamsUtils.toParamsMap(AccountUtils.accountPath);
        } catch (FileNotFoundException ignored) {
            accountMap = new HashMap<>();
        }
        if (accountName == null) {
            accountName = accountMap.get("account");
            if (accountName == null) throw new IOException("no default account.");
        }
        if (accountName.contains("-")) {
            String[] keys = new String[3];
            int index = accountName.indexOf("-");
            String source = accountName.substring(0, index);
            String account;
            switch (source) {
                case "qiniu": keys[2] = CloudApiUtils.QINIU;
                    account = String.join("-", accountName.substring(index + 1), CloudApiUtils.QINIU); break;
                case "ten": keys[2] = CloudApiUtils.TENCENT;
                    account = String.join("-", accountName.substring(index + 1), CloudApiUtils.TENCENT); break;
                case "ali": keys[2] = CloudApiUtils.ALIYUN;
                    account = String.join("-", accountName.substring(index + 1), CloudApiUtils.ALIYUN); break;
                case "up": keys[2] = CloudApiUtils.UPYUN;
                    account = String.join("-", accountName.substring(index + 1), CloudApiUtils.UPYUN); break;
                case "aws":
                case "s3": keys[2] = CloudApiUtils.AWSS3;
                    account = String.join("-", accountName.substring(index + 1), CloudApiUtils.AWSS3); break;
                case "bai": keys[2] = CloudApiUtils.BAIDU;
                    account = String.join("-", accountName.substring(index + 1), CloudApiUtils.BAIDU); break;
                case "hua": keys[2] = CloudApiUtils.HUAWEI;
                    account = String.join("-", accountName.substring(index + 1), CloudApiUtils.HUAWEI); break;
                default:
                    if (accountName.endsWith("-aws")) {
                        account = accountName.substring(0, accountName.length() - 4) + "-s3";
                    } else {
                        account = accountName;
                    }
                    keys[2] = accountName.substring(index + 1);
                    break;
            }
            keys[0] = accountMap.get(String.join("-", account, "id"));
            keys[1] = accountMap.get(String.join("-", account, "secret"));
            if (keys[0] != null && keys[1] != null) {
                keys[0] = new String(decoder.decode(keys[0].substring(8)));
                if (secretMode) {
                    keys[1] = "************************";
                } else {
                    keys[1] = new String(decoder.decode(keys[1].substring(8)));
                }
            } else {
                throw new IOException("no account: " + accountName);
            }
            return new ArrayList<String[]>(1){{ add(keys); }};
        } else {
            List<String[]> keysList = new ArrayList<>();
            String[] keys = getAccount(accountMap, accountName, CloudApiUtils.QINIU, secretMode);
            if (keys[0] != null) keysList.add(keys);
            keys = getAccount(accountMap, accountName, CloudApiUtils.TENCENT, secretMode);
            if (keys[0] != null) keysList.add(keys);
            keys = getAccount(accountMap, accountName, CloudApiUtils.ALIYUN, secretMode);
            if (keys[0] != null) keysList.add(keys);
            keys = getAccount(accountMap, accountName, CloudApiUtils.AWSS3, secretMode);
            if (keys[0] != null) keysList.add(keys);
            keys = getAccount(accountMap, accountName, CloudApiUtils.UPYUN, secretMode);
            if (keys[0] != null) keysList.add(keys);
            keys = getAccount(accountMap, accountName, CloudApiUtils.HUAWEI, secretMode);
            if (keys[0] != null) keysList.add(keys);
            keys = getAccount(accountMap, accountName, CloudApiUtils.BAIDU, secretMode);
            if (keys[0] != null) keysList.add(keys);
            if (keysList.size() == 0) {
                throw new IOException("no account: " + accountName);
            }
            return keysList;
        }
    }

    public static String[] getAccount(Map<String, String> accountMap, String accountName, String source, boolean secretMode) {
        String[] keys = new String[3];
        keys[2] = source;
        if ("aws".equals(source)) source = CloudApiUtils.AWSS3;
        keys[0] = accountMap.get(String.join("-", accountName, source, "id"));
        if (keys[0] != null) {
            keys[0] = new String(decoder.decode(keys[0].substring(8)));
            if (secretMode) {
                keys[1] = "************************";
            } else {
                keys[1] = accountMap.get(String.join("-", accountName, source, "secret"));
                keys[1] = new String(decoder.decode(keys[1].substring(8)));
            }
        }
        return keys;
    }
}
