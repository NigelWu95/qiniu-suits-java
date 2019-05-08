package com.qiniu.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import static okhttp3.Dns.SYSTEM;

public final class RequestUtils {

    public static String lookUpFirstIpFromHost(String domain) throws UnknownHostException {
        if (domain == null || "".equals(domain)) throw new UnknownHostException("the hostname is empty.");
        List<InetAddress> inetAddresses = SYSTEM.lookup(domain);
        return inetAddresses.get(0).getHostAddress();
    }
}
