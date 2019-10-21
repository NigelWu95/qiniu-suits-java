package com.qiniu.util;

import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class AccountUtilsTest {

    @Test
    public void testGetAccount() throws IOException {
        Map<String, String> accountMap = ParamsUtils.toParamsMap(AccountUtils.accountPath);
        String[] keys1 = AccountUtils.getAccount(accountMap, "wbh", "qiniu", true);
        System.out.println(keys1[0] + "  " + keys1[1]);
        keys1 = AccountUtils.getAccount(accountMap, "wbh-qiniu", "qiniu", true);
        System.out.println(keys1[0] + "  " + keys1[1]);
        String[] keys2 = AccountUtils.getAccount(accountMap, "test", "qiniu", true);
        System.out.println(keys2[0] + "  " + keys2[1]);
        keys2 = AccountUtils.getAccount(accountMap, "test-qiniu", "qiniu", true);
        System.out.println(keys2[0] + "  " + keys2[1]);
        String[] keys3 = AccountUtils.getAccount(accountMap, null, "qiniu", false);
        System.out.println(keys3[0] + "  " + keys3[1]);
    }

}