package com.qiniu.util;

import com.qiniu.sdk.UpYunConfig;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;

public final class CharactersUtils {

    /**
     * 对字符串进行 MD5 加密
     *
     * @param str 待加密字符串
     * @return 加密后字符串
     */
    public static String md5(String str) throws Exception {
        char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                'a', 'b', 'c', 'd', 'e', 'f'};
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update(str.getBytes(UpYunConfig.UTF8));
        byte[] encodedValue = md5.digest();
        int j = encodedValue.length;
        char finalValue[] = new char[j * 2];
        int k = 0;
        for (byte encoded : encodedValue) {
            finalValue[k++] = hexDigits[encoded >> 4 & 0xf];
            finalValue[k++] = hexDigits[encoded & 0xf];
        }

        return new String(finalValue);
    }

    public static String[] parseStringToHexArray(String originString) {

        byte[] byteArray = originString.getBytes();
        return parseBytesToHexArray(byteArray);
    }

    public static String[] parseBytesToHexArray(byte[] byteArray) {

        String hex;
        StringBuilder hexString = new StringBuilder();

        for (int i = 0; i < byteArray.length; i++) {
            if (i > 0)
                hexString.append(",");

            hex = Integer.toHexString(0xFF & byteArray[i]);
            if ((byteArray[i] & 0xFF) < 0x10) // 0~F前面加上零
                hexString.append("0x0").append(hex);
            else
                hexString.append("0x").append(hex);
        }

        return hexString.toString().split(",");
    }

    public static String bytesToHexString(byte[] byteArray) {
        String result = "";
        for (int i = 0; i < byteArray.length; i++) {
            String temp = Integer.toHexString(byteArray[i] & 0xff);
            if (temp.length() == 1) {
                temp = "0" + temp;
            }
            result += temp;
        }

        return result;
    }

    public static byte[] hexStringToBytes(String hexString) {
        int len = (hexString.length() / 2);
        byte[] result = new byte[len];
        char[] achar = hexString.toCharArray();
        for (int i = 0; i < len; i++) {
            int pos = i * 2;
            result[i] = (byte) (toByte(achar[pos]) << 4 | toByte(achar[pos + 1]));
        }
        return result;
    }

    private static int toByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }

    /*
       将 16 进制字符串转换为字节数组
     */
    public static byte[] decode(char[] data) throws IllegalArgumentException {
        int len = data.length;
        // data 的长度不能为奇数
        if ((len & 0x01) != 0) {
            throw new IllegalArgumentException("Odd number of characters.");
        }
        byte[] out = new byte[len >> 1];
        // two characters form the hex value.
        for (int i = 0, j = 0; j < len; i++) {
            int digit = toDigit(data[j], j) << 4;
            j++;
            digit = digit | toDigit(data[j], j);
            j++;
            out[i] = (byte) (digit & 0xFF);
        }
        return out;
    }

    public static int toDigit(char ch, int index) throws IllegalArgumentException {
        int digit = Character.digit(ch, 16);
        if (digit == -1) {
            throw new IllegalArgumentException("Illegal hexadecimal character " + ch + " at index " + index);
        }
        return digit;
    }

    public static byte[] getBytes(char[] chars) {
        Charset cs = Charset.forName("UTF-8");
        CharBuffer cb = CharBuffer.allocate(chars.length);
        cb.put(chars);
        cb.flip();
        ByteBuffer byteBuffer = cs.encode(cb);
        return byteBuffer.array();
    }
}
