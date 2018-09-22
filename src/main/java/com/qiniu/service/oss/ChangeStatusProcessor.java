package com.qiniu.service.oss;

import com.qiniu.common.QiniuAuth;
import com.qiniu.common.QiniuException;
import com.qiniu.common.QiniuSuitsException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.util.StringMap;
import com.qiniu.util.UrlSafeBase64;

public class ChangeStatusProcessor {

    private QiniuAuth auth;
    private Client client;

    private static volatile ChangeStatusProcessor changeStatusProcessor = null;

    public ChangeStatusProcessor(QiniuAuth auth, Client client) {
        this.auth = auth;
        this.client = client;
    }

    public static ChangeStatusProcessor getChangeStatusProcessor(QiniuAuth auth, Client client) {
        if (changeStatusProcessor == null) {
            synchronized (ChangeStatusProcessor.class) {
                if (changeStatusProcessor == null) {
                    changeStatusProcessor = new ChangeStatusProcessor(auth, client);
                }
            }
        }
        return changeStatusProcessor;
    }

    public String doStatusChange(String bucket, String key, short status) throws QiniuSuitsException {

        Response response = null;
        String respBody = "";
        String url = "http://rs.qiniu.com/chstatus/" + UrlSafeBase64.encodeToString(bucket + ":" + key) + "/status/" + status;
        String accessToken = "QBox " + auth.signRequest(url, null, Client.FormMime);
        StringMap headers = new StringMap();
        headers.put("Authorization", accessToken);

        try {
            response = client.post(url, null, headers, Client.FormMime);
            respBody = response.bodyString();
        } catch (QiniuException e) {
            QiniuSuitsException qiniuSuitsException = new QiniuSuitsException("change status error");
            qiniuSuitsException.addToFieldMap("code", String.valueOf(e.code()));
            qiniuSuitsException.addToFieldMap("error", String.valueOf(e.error()));
            qiniuSuitsException.setStackTrace(e.getStackTrace());
            throw qiniuSuitsException;
        } finally {
            if (response != null)
                response.close();
        }

        return response.statusCode + "\t" + response.reqId + "\t" + respBody;
    }
}