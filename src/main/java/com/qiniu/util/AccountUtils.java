package com.qiniu.util;

import com.qiniu.interfaces.IEntryParam;

import java.io.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

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
        } else if (account.startsWith("s3-")) {
            account = account.substring(3);
            id = account + "-s3-id=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("s3-id").getBytes()));
            secret = account + "-s3-secret=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("s3-secret").getBytes()));
        } else if (account.startsWith("aws-")) {
            account = account.substring(4);
            id = account + "-aws-id=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("aws-id").getBytes()));
            secret = account + "-aws-secret=" + EncryptUtils.getRandomString(8) +
                    new String(encoder.encode(entryParam.getValue("aws-secret").getBytes()));
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

    public static List<String[]> getAccount(String accountName, boolean secretMode) throws IOException {
        Map<String, String> accountMap = ParamsUtils.toParamsMap(AccountUtils.accountPath);
        if (accountName == null) {
            accountName = accountMap.get("account");
            if (accountName == null) throw new IOException("no default account.");
        }
        if (accountName.contains("-")) {
            String[] keys = new String[3];
            keys[0] = accountMap.get(String.join("-", accountName, "id"));
            keys[1] = accountMap.get(String.join("-", accountName, "secret"));
            if (keys[0] != null && keys[1] != null) {
                keys[0] = new String(decoder.decode(keys[0].substring(8)));
                if (secretMode) {
                    keys[1] = "************************";
                } else {
                    keys[1] = new String(decoder.decode(keys[1].substring(8)));
                }
                keys[2] = accountName.substring(accountName.indexOf("-") + 1);
            } else {
                throw new IOException("no account: " + accountName);
            }
            return new ArrayList<String[]>(1){{ add(keys); }};
        } else {
            List<String[]> keysList = new ArrayList<>();
            String[] keys = getAccount(accountMap, accountName, "qiniu", secretMode);
            if (keys[0] != null) keysList.add(keys);
            keys = getAccount(accountMap, accountName, "tencent", secretMode);
            if (keys[0] != null) keysList.add(keys);
            keys = getAccount(accountMap, accountName, "aliyun", secretMode);
            if (keys[0] != null) keysList.add(keys);
            keys = getAccount(accountMap, accountName, "aws", secretMode);
            if (keys[0] != null) keysList.add(keys);
            keys = getAccount(accountMap, accountName, "s3", secretMode);
            if (keys[0] != null) keysList.add(keys);
            keys = getAccount(accountMap, accountName, "upyun", secretMode);
            if (keys[0] != null) keysList.add(keys);
            keys = getAccount(accountMap, accountName, "huawei", secretMode);
            if (keys[0] != null) keysList.add(keys);
            keys = getAccount(accountMap, accountName, "baidu", secretMode);
            if (keys[0] != null) keysList.add(keys);
            if (keysList.size() == 0) {
                throw new IOException("no account: " + accountName);
            }
            return keysList;
        }
    }

    public static String[] getAccount(Map<String, String> accountMap, String accountName, String source, boolean secretMode) {
        String[] keys = new String[3];
        keys[0] = accountMap.get(String.join("-", accountName, source, "id"));
        if (keys[0] != null) {
            keys[0] = new String(decoder.decode(keys[0].substring(8)));
            if (secretMode) {
                keys[1] = "************************";
            } else {
                keys[1] = accountMap.get(String.join("-", accountName, source, "secret"));
                keys[1] = new String(decoder.decode(keys[1].substring(8)));
            }
            keys[2] = source;
        }
        return keys;
    }
}
