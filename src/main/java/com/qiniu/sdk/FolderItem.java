package com.qiniu.sdk;

import java.util.Date;

public class FolderItem {

    // 文件名
    public String key;

    // 文件属性 {file, folder}
    public String attribute;

    // 文件大小
    public long size;

    // 文件日期
    public Date date;

    public FolderItem(String data) {
        String[] a = data.split("\t");
        if (a.length == 4) {
            this.key = a[0];
            this.attribute = ("N".equals(a[1]) ? "File" : "Folder");
            try {
                this.size = Long.parseLong(a[2].trim());
            } catch (NumberFormatException e) {
                this.size = -1;
            }
            long da = 0;
            try {
                da = Long.parseLong(a[3].trim());
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
            this.date = new Date(da * 1000);
        }
    }

    @Override
    public String toString() {
        return "key=" + key + ",size=" + size + ",time=" + date + ",attribute=" + attribute;
    }
}
