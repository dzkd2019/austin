package com.java3y.austin.handler.deduplication.limit;

import com.java3y.austin.common.domain.TaskInfo;
import com.java3y.austin.handler.deduplication.DeduplicationParam;
import com.java3y.austin.handler.deduplication.service.AbstractDeduplicationService;
import com.java3y.austin.support.utils.RedisUtils;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 滑动窗口去重器（内容去重采用基于redis中zset的滑动窗口去重，可以做到严格控制单位时间内的频次。）
 * 业务逻辑：5分钟内相同用户如果收到相同的内容，则应该被过滤掉
 * 技术方案：由lua脚本实现
 *
 * @author cao
 * @date 2022-04-20 11:34
 */
@Service(value = "SlideWindowLimitService")
public class SlideWindowLimitService extends AbstractLimitService {

    private static final String LIMIT_TAG = "SW_";

    @Autowired
    private RedisUtils redisUtils;


    private DefaultRedisScript<Long> redisScript;

    private static final int BATCH_SIZE = 1000;


    @PostConstruct
    public void init() {
        redisScript = new DefaultRedisScript<>();
        redisScript.setResultType(Long.class);
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("limit.lua")));
    }


    /**
     * @param service  去重器对象
     * @param taskInfo
     * @param param    去重参数
     * @return 返回不符合条件的手机号码
     */
    @Override
    public Set<String> limitFilter(AbstractDeduplicationService service, TaskInfo taskInfo, DeduplicationParam param) {

        Set<String> filterReceiver = new HashSet<>(taskInfo.getReceiver().size());
        List<String> receivers = new ArrayList<>(taskInfo.getReceiver());


        for (int i = 0; i < receivers.size(); i += BATCH_SIZE) {
            List<String> batch = receivers.subList(i, Math.min(i + BATCH_SIZE, receivers.size()));
            List<String> keys = batch
                    .stream()
                    .map(receiver -> LIMIT_TAG + deduplicationSingleKey(service, taskInfo, receiver))
                    .toList();

            List<Boolean> filterResult = redisUtils.execLimitLuaPipeline(redisScript, keys, param.getDeduplicationTime() * 1000, param.getCountNum());

            for (int j = 0; j < batch.size(); j++) {
                if (filterResult.get(j)) {
                    filterReceiver.add(batch.get(j));
                }
            }
        }

        return filterReceiver;
    }


}
