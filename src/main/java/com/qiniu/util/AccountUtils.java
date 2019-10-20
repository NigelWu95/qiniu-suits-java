package com.qiniu.util;

import com.qiniu.interfaces.IEntryParam;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;

public class AccountUtils {

    public static String accountPath = String.join(FileUtils.pathSeparator, "~", ".qsuits.account");

    public static void setAccount(IEntryParam entryParam, String account) throws Exception {
        String filePath = FileUtils.convertToRealPath("~" + FileUtils.pathSeparator + ".qsuits.account");
        File accountFile = new File(filePath);
        boolean accountFileExists = (!accountFile.isDirectory() && accountFile.exists()) || accountFile.createNewFile();
        if (!accountFileExists) throw new IOException("account file not exists and can not be created.");
        String id;
        String secret;
        java.util.Base64.Encoder encoder = Base64.getEncoder();
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
