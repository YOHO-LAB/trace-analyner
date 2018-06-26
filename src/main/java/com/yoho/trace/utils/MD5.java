package com.yoho.trace.utils;

import org.apache.commons.codec.Charsets;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MD5 工具类
 * Created by chzhang@yoho.cn on 2015/11/6.
 */
public class MD5 {

    private static final Logger logger = LoggerFactory.getLogger(MD5.class);

    /**
     * 对消息计算MD5， 结果用Hex（十六进制）编码
     *
     * @param message 消息
     * @return MD5之后的结果
     */
    public static String md5(String message) {
        return new String(Hex.encodeHex(md5Digest(message)));
    }

    /**
     * 计算MD5
     *
     * @param message 原始消息
     * @return MD5之后的记过
     */
    private static byte[] md5Digest(String message) {

        byte[] md5Bytes = null;

        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md5Bytes = md.digest(message.getBytes(Charsets.UTF_8));

        } catch (NoSuchAlgorithmException e) {
            logger.error("md5 error: NoSuchAlgorithmException");
        }

        return md5Bytes;
    }


}
