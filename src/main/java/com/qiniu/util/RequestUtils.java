package com.qiniu.util;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

public final class RequestUtils {

    public static Client client = new Client();

    public static String lookUpFirstIpFromHost(String domain) throws UnknownHostException {
        if (domain == null || "".equals(domain)) throw new UnknownHostException("the hostname is empty.");
        try {
            List<InetAddress> inetAddresses = Arrays.asList(InetAddress.getAllByName(domain));
            return inetAddresses.get(0).getHostAddress();
        } catch (NullPointerException e) {
            UnknownHostException unknownHostException =
                    new UnknownHostException("Broken system behaviour for dns lookup of " + domain);
            unknownHostException.initCause(e);
            throw unknownHostException;
        }
    }

    public static void checkCallbackUrl(String callbackUrl) throws QiniuException {
        Response response = client.post(callbackUrl, null, null, null);
        response.close();
    }
}
