package com.qiniu.util;

import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AccountUtilsTest {

    @Test
    public void testGetAccount() throws IOException {

        List<String[]> keysList = AccountUtils.getAccount("wbh", false);
        for (String[] keys : keysList) {
            System.out.println(keys[2] + ": ");
            System.out.println("id: " + keys[0]);
            System.out.println("secret: " + keys[1]);
        }

        Map<String, String> accountMap = ParamsUtils.toParamsMap(AccountUtils.accountPath);
        String[] keys1 = AccountUtils.getAccount(accountMap, "wbh", "qiniu", true);
        System.out.println(keys1[0] + "  " + keys1[1]);
        keys1 = AccountUtils.getAccount(accountMap, "wbh", "aws", true);
        System.out.println(keys1[0] + "  " + keys1[1]);
        String[] keys2 = AccountUtils.getAccount(accountMap, "test", "qiniu", true);
        System.out.println(keys2[0] + "  " + keys2[1]);
        keys2 = AccountUtils.getAccount(accountMap, "test-qiniu", "qiniu", true);
        System.out.println(keys2[0] + "  " + keys2[1]);
        String[] keys3 = AccountUtils.getAccount(accountMap, null, "qiniu", false);
        System.out.println(keys3[0] + "  " + keys3[1]);
    }

}