package com.qiniu.util;

import com.qiniu.service.process.ListFileAntiFilter;
import com.qiniu.service.process.ListFileFilter;

public class ListFileFilterUtils {

    public static boolean checkListFileFilter(ListFileFilter listFileFilter) {

        return ((listFileFilter != null && listFileFilter.isValid()));
    }

    public static boolean checkListFileAntiFilter(ListFileAntiFilter listFileAntiFilter) {

        return ((listFileAntiFilter != null && listFileAntiFilter.isValid()));
    }
}
