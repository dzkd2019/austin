package com.java3y.austin.web.config;

import com.alibaba.fastjson2.support.config.FastJsonConfig;
import com.alibaba.fastjson2.support.spring6.http.converter.FastJsonHttpMessageConverter;
import com.google.common.collect.Lists;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverters;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * @author kl
 * @version 1.0.0
 * @description 通用配置
 * @date 2023/2/23 10:40
 */
@Configuration
public class FastJsonConfiguration implements WebMvcConfigurer {

    /**
     * FastJson 消息转换器 格式化输出json
     *
     * @return
     */
    @Override
    public void configureMessageConverters(HttpMessageConverters.ServerBuilder builder) {
        FastJsonHttpMessageConverter converter = new FastJsonHttpMessageConverter();
        FastJsonConfig config = new FastJsonConfig();

        config.setDateFormat("yyyy-MM-dd HH:mm:ss");

        config.setWriterFeatures(
                com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue,
                com.alibaba.fastjson2.JSONWriter.Feature.PrettyFormat
        );

        converter.setFastJsonConfig(config);
        converter.setSupportedMediaTypes(Lists.newArrayList(MediaType.APPLICATION_JSON));
        builder.configureMessageConvertersList(converts -> converts.addFirst(converter));
    }
}
