package com.qiniu.util;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.*;

public class FileUtilsTest {

    @Test
    public void testContentType() throws IOException {
        File file1 = new File("/Users/wubingheng/Downloads/sign.py");
        File file2 = new File("/Users/wubingheng/Downloads/cdn-qiniu.html");
        File file3 = new File("/Users/wubingheng/Downloads/IAM文档 20190514.pdf");
        File file4 = new File("/Users/wubingheng/Downloads/eaf79854b71a754f87d723df9aa73d02.jpg");
        File file5 = new File("/Users/wubingheng/Downloads/20190525_00-20190530_08.numbers");
        File file6 = new File("/Users/wubingheng/Downloads/OPPO-百度功能验收报告v1.2.docx");
        File file7 = new File("/Users/wubingheng/Projects/Github/temp/xaz");
        System.out.println(FileUtils.contentType(file1));
        System.out.println(FileUtils.contentType(file2));
        System.out.println(FileUtils.contentType(file3));
        System.out.println(FileUtils.contentType(file4));
        System.out.println(FileUtils.contentType(file5));
        System.out.println(FileUtils.contentType(file6));
        System.out.println(FileUtils.contentType(file7));
//        System.out.println(FileUtils.contentType("/Users/wubingheng/Downloads/sign.py"));
//        System.out.println(FileUtils.contentType("/Users/wubingheng/Downloads/cdn-qiniu.html"));
//        System.out.println(FileUtils.contentType("/Users/wubingheng/Downloads/IAM文档 20190514.pdf"));
//        System.out.println(FileUtils.contentType("/Users/wubingheng/Downloads/eaf79854b71a754f87d723df9aa73d02.jpg"));
//        System.out.println(FileUtils.contentType("/Users/wubingheng/Downloads/20190525_00-20190530_08.numbers"));
//        System.out.println(FileUtils.contentType("/Users/wubingheng/Downloads/OPPO-百度功能验收报告v1.2.docx"));
//        System.out.println(FileUtils.contentType("/Users/wubingheng/Projects/Github/temp/xaz"));
        System.out.println(new File("./Downloads").getCanonicalPath());
        System.out.println(new File("./Downloads").getAbsolutePath());
        System.out.println(new File("~/Downloads").getCanonicalPath());
        System.out.println(new File("~/Downloads").getAbsolutePath());
        System.out.println(new File("../Downloads").getCanonicalPath());
        System.out.println(new File("../Downloads").getAbsolutePath());
        System.out.println(new File("~/Downloads").getParent());
        System.out.println(new File("~/Downloads").getName());
        System.out.println(new File("~/Downloads").getPath());
        System.out.println(System.getProperty("user.home") + System.getProperty("file.separator") + "Downloads");
    }

    @Test
    public void test() {
        File file = new File("/Users/wubingheng/Downloads/react.txt");
        System.out.println(file.exists());
        System.out.println(file.getFreeSpace());
        System.out.println(file.getTotalSpace());
        System.out.println(file.getUsableSpace());
        System.out.println(file.isHidden());
        System.out.println(file.lastModified());
        System.out.println(file.length());
        File[] files = file.listFiles();
//        System.out.println(files.length);
        File[] files2 = new File("/Users/wubingheng/Downloads/").listFiles();
        System.out.println(files2.length);
    }

    @Test
    public void testConvertToRealPath() throws IOException {
        String path = FileUtils.convertToRealPath("\\~/Downloads");
        File file1 = new File(path);
        System.out.println(file1.getCanonicalPath());
        File file2 = new File("~/Downloads");
        System.out.println(file2.getCanonicalPath());
        File file3 = new File("\\~/Downloads");
        System.out.println(file3.getCanonicalPath());
    }

    @Test
    public void testLastLineOfFile() throws IOException {
        System.out.println("test");
        System.out.println(FileUtils.lastLineOfFile("pom.xml"));
//        System.out.println(FileUtils.lastLineOfFile("qsuits.iml"));
        System.out.println(FileUtils.lastLineOfFile("/Users/wubingheng/Downloads/redis-work.md"));
    }
}