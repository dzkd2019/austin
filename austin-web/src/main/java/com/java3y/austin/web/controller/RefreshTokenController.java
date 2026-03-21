package com.java3y.austin.web.controller;


import com.java3y.austin.common.enums.ChannelType;
import com.java3y.austin.cron.handler.RefreshDingDingAccessTokenHandler;
import com.java3y.austin.cron.handler.RefreshGeTuiAccessTokenHandler;
import com.java3y.austin.web.annotation.AustinAspect;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * @author 3y
 */

@AustinAspect
@Tag(name = "手动刷新token的接口")
@RestController
public class RefreshTokenController {


    @Autowired
    private RefreshDingDingAccessTokenHandler refreshDingDingAccessTokenHandler;
    @Autowired
    private RefreshGeTuiAccessTokenHandler refreshGeTuiAccessTokenHandler;

    /**
     * 按照不同的渠道刷新对应的Token，channelType取值来源com.java3y.austin.common.enums.ChannelType
     *
     * @param channelType
     * @return
     */
    @Operation(summary = "手动刷新token", description = "钉钉/个推 token刷新")
    @GetMapping("/refresh")
    public String refresh(Integer channelType) {
        if (ChannelType.PUSH.getCode().equals(channelType)) {
            refreshGeTuiAccessTokenHandler.execute();
        }
        if (ChannelType.DING_DING_WORK_NOTICE.getCode().equals(channelType)) {
            refreshDingDingAccessTokenHandler.execute();

        }
        return "刷新成功";
    }

}
