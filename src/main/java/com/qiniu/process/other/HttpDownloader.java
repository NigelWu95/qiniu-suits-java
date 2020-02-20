package com.qiniu.process.other;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.FileUtils;
import com.qiniu.util.StringMap;

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
        this.client = configuration == null ? new Client() : new Client(configuration.clone());
    }

    public File download(String url, String filepath, StringMap headers) throws IOException {
        Response response = client.get(url, headers);
        if (response.statusCode == 200 || response.statusCode == 206) {
            File file = new File(filepath);
            if (file.exists()) {
                throw new IOException(String.join("", "file: ", filepath, " is already exists."));
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
            return file;
        } else {
            throw new QiniuException(response);
        }
    }

    public File download(String url, File file, boolean append, StringMap headers) throws IOException {
        Response response = client.get(url, headers);
        if (response.statusCode == 200 || response.statusCode == 206) {
            byte[] buffer = new byte[4096];
            int byteRead;
            try (FileOutputStream writer = new FileOutputStream(file, append);
                 InputStream inputStream = response.bodyStream()) {
                while ((byteRead = inputStream.read(buffer)) > -1) {
                    writer.write(buffer, 0, byteRead);
                }
            } finally {
                response.close();
            }
            return file;
        } else {
            throw new QiniuException(response);
        }
    }

    public Response downloadResponse(String url, StringMap headers) throws IOException {
        Response response = client.get(url, headers);
        if (response.statusCode == 200 || response.statusCode == 206) {
            return response;
        } else {
            throw new QiniuException(response);
        }
    }

    public void download(String url, StringMap headers) throws IOException {
        Response response = downloadResponse(url, headers);
        byte[] buffer = new byte[4096];
        try (InputStream inputStream = response.bodyStream()) {
            while ((inputStream.read(buffer)) > -1);
        } finally {
            response.close();
        }
    }
}
