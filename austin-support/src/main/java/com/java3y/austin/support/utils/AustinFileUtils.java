package com.java3y.austin.support.utils;

import cn.hutool.core.io.IoUtil;
import com.java3y.austin.common.constant.CommonConstant;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author 3y
 * @date 2023/2/14
 */
@Slf4j
public class AustinFileUtils {

    private AustinFileUtils() {

    }

    /**
     * 读取 远程链接 返回File对象
     *
     * @param path      文件路径
     * @param remoteUrl cdn/oss文件访问链接
     * @return
     */
    public static File getRemoteUrl2File(String path, String remoteUrl) {
        try {
            URL url = new URL(remoteUrl);
            String protocol = url.getProtocol();
            // 防止SSRF攻击
            if (!CommonConstant.HTTP.equalsIgnoreCase(protocol)
                    && !CommonConstant.HTTPS.equalsIgnoreCase(protocol)
                    && !CommonConstant.OSS.equalsIgnoreCase(protocol)) {
                log.error("AustinFileUtils#getRemoteUrl2File fail:{}, remoteUrl:{}",
                        "The remoteUrl is invalid, it can only be of the types http, https, and oss.", remoteUrl);
                return null;
            }
            File file = new File(path, url.getPath());
            if (!file.exists()) {
                boolean res = file.getParentFile().mkdirs();
                if (!res) {
                    log.error("AustinFileUtils#getRemoteUrl2File Failed to create folder, path:{}, remoteUrl:{}", path, remoteUrl);
                    return null;
                }
                try (InputStream inputStream = url.openStream();
                     FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                    IoUtil.copy(inputStream, fileOutputStream);
                }
            }
            return file;
        } catch (Exception e) {
            log.error("AustinFileUtils#getRemoteUrl2File fail, remoteUrl:{}", remoteUrl, e);
        }
        return null;
    }

    /**
     * 读取 远程链接集合 返回有效的File对象集合
     *
     * @param path       文件路径
     * @param remoteUrls cdn/oss文件访问链接集合
     * @return
     */
    public static List<File> getRemoteUrl2File(String path, Collection<String> remoteUrls) {
        List<File> files = new ArrayList<>();
        remoteUrls.forEach(remoteUrl -> {
            File file = getRemoteUrl2File(path, remoteUrl);
            if (file != null) {
                files.add(file);
            }
        });
        return files;
    }

}
