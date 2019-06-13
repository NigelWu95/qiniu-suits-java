package com.qiniu.sdk;

public class FileItem {

    // 文件名
    public String key;

    // 文件属性 {file, folder}
    public String attribute;

    // 文件大小
    public long size;

    // 文件日期
//    public Date date;
    public long timeSeconds;

    public FileItem() {}

    public FileItem(String data) {
        String[] a = data.split("\t");
        key = a[0];
        if (a.length > 1) attribute = ("N".equals(a[1]) ? "File" : "Folder");
        if (a.length > 2) size = Long.parseLong(a[2].trim());
        if (a.length > 3) timeSeconds = Long.parseLong(a[3].trim());
//        try {
//            this.size = Long.parseLong(a[2].trim());
//        } catch (NumberFormatException e) {
//            this.size = -1;
//        }
//        long da = 0;
//        try {
//            da = Long.parseLong(a[3].trim());
//        } catch (NumberFormatException e) {
//            e.printStackTrace();
//        }
//        this.date = new Date(da * 1000);
    }

    @Override
    public String toString() {
//        return "key=" + key + ",size=" + size + ",time=" + date + ",attribute=" + attribute;
        return "key=" + key + ",size=" + size + ",time=" + timeSeconds + ",attribute=" + attribute;
    }
}
