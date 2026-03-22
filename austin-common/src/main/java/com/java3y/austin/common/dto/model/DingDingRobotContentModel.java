package com.java3y.austin.common.dto.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 3y
 * 钉钉 自定义机器人
 * <p>
 * <a href="<a">href="https://open.dingtalk.com/document/group/custom-rob</a>ot-access">...</a>
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DingDingRobotContentModel extends ContentModel {

    /**
     * 发送类型
     */
    private String sendType;

    /**
     * 钉钉机器人：【文本消息】内容，【markdown消息】内容，【ActionCard消息】内容
     */
    private String content;

    /**
     * 钉钉机器人：【markdown消息】标题，【FeedCard消息】标题，【ActionCard消息】标题
     */
    private String title;

    /**
     * 钉钉机器人：【ActionCard消息】按钮布局
     */
    private String btnOrientation;

    /**
     * 钉钉机器人：【ActionCard消息】按钮的文案和跳转链接的json
     * [{\"title\":\"别点我\",\"actionURL\":\"<a href="https://www.baidu.com/">...</a>\"},{\"title\":\"没关系，还是点我把\",\"actionURL\":\"<a href="https://www.baidu.com/">...</a>\\t\"}]
     */
    private String btns;


    /**
     * 钉钉机器人：【链接消息】点击消息跳转的URL，【FeedCard消息】点击消息跳转的URL
     */
    private String url;

    /**
     * 钉钉机器人：【链接消息】图片URL，【FeedCard消息】图片URL
     */
    private String picUrl;


    /**
     * 钉钉机器人：【FeedCard类型】 消息体
     * "[{\"picUrl\":\"<a href="https://img.alicdn.com/tfs/TB1NwmBEL9TBuNjy1zbXXXpepXa-2400-1218.png">...</a>\",\"title\":\"{$title1}\",\"url\":\"<a href="https://www.dingtalk.com/">...</a>\"},{\"picUrl\":\"<a href="https://img.alicdn.com/tfs/TB1NwmBEL9TBuNjy1zbXXXpepXa-2400-1218.png">...</a>\\t\",\"title\":\"时代的火车向前开2\",\"url\":\"<a href="https://www.dingtalk.com/">...</a>\"}]"}
     */
    private String feedCards;
}
