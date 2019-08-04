package com.qiniu.process.other;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.FileUtils;

import java.io.*;

public class HttpDownloader {

    private Client client;

    public HttpDownloader() {
        Configuration configuration = new Configuration();
        configuration.connectTimeout = 60;
        configuration.readTimeout = 1200;
        configuration.writeTimeout = 60;
        this.client = new Client(configuration);
    }

    public HttpDownloader(Configuration configuration) {
        this.client = new Client(configuration);
    }

    public void download(String url, String filepath) throws IOException {
        Response response = client.get(url);
        if (response.statusCode >= 200 && response.statusCode < 300) {
            File file = new File(filepath);
            if (file.exists()) {
                throw new IOException("file: " + filepath + " is already exists.");
            } else {
                boolean exists = FileUtils.mkDirAndFile(file);
                while (!exists) {
                    exists = FileUtils.mkDirAndFile(file);
                }
            }
            byte[] buffer = new byte[4096];
            int byteRead;
            try (FileOutputStream writer = new FileOutputStream(file);
                 InputStream inputStream = response.bodyStream()) {
                while ((byteRead = inputStream.read(buffer)) > -1) {
                    writer.write(buffer, 0, byteRead);
                }
            } finally {
                response.close();
            }
        } else {
            throw new QiniuException(response);
        }
    }
}
