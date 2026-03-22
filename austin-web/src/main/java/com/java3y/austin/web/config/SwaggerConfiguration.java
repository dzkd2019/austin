package com.java3y.austin.web.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * SpringDoc OpenAPI配置类
 * 替代Springfox Swagger
 *
 * @author 3y
 */
@Configuration
public class SwaggerConfiguration {

    /**
     * 配置OpenAPI元信息
     *
     * @return OpenAPI配置
     */
    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("austin平台")
                        .description("消息推送接口文档")
                        .version("v1.0")
                        .contact(new Contact()
                                .name("3y")
                                .url("http://gitee.com/zhongfucheng/austin")
                                .email("403686131@qq.com")));
    }

    /**
     * 用户端接口文档组
     * 地址：<a href="http://localhost:8080/swagger-ui/index.html">...</a>
     *
     * @return GroupedOpenApi配置
     */
    @Bean
    public GroupedOpenApi webApi() {
        return GroupedOpenApi.builder()
                .group("用户端接口文档")
                .pathsToMatch("/**")
                .packagesToScan("com.java3y.austin.web.controller")
                .build();
    }

}
