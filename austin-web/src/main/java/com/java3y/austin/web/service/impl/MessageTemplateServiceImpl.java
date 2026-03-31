package com.java3y.austin.web.service.impl;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.ObjectUtil;
import com.java3y.austin.common.constant.AustinConstant;
import com.java3y.austin.common.constant.CommonConstant;
import com.java3y.austin.common.enums.AuditStatus;
import com.java3y.austin.common.enums.MessageStatus;
import com.java3y.austin.common.enums.RespStatusEnum;
import com.java3y.austin.common.enums.TemplateType;
import com.java3y.austin.common.vo.BasicResultVO;
import com.java3y.austin.cron.xxl.entity.XxlJobInfo;
import com.java3y.austin.cron.xxl.service.CronTaskService;
import com.java3y.austin.cron.xxl.utils.XxlJobUtils;
import com.java3y.austin.support.cache.MessageTemplateCaching;
import com.java3y.austin.support.dao.MessageTemplateDao;
import com.java3y.austin.support.domain.MessageTemplate;
import com.java3y.austin.web.service.MessageTemplateService;
import com.java3y.austin.web.vo.MessageTemplateParam;
import jakarta.persistence.criteria.Predicate;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 消息模板管理 Service
 *
 * @author 3y
 * @date 2022/1/22
 */
@Service
@Slf4j
public class MessageTemplateServiceImpl implements MessageTemplateService {


    private final MessageTemplateDao messageTemplateDao;

    private final CronTaskService cronTaskService;

    private final XxlJobUtils xxlJobUtils;

    private final MessageTemplateCaching cache;

    public MessageTemplateServiceImpl(MessageTemplateDao messageTemplateDao, CronTaskService cronTaskService, XxlJobUtils xxlJobUtils, MessageTemplateCaching cache) {
        this.messageTemplateDao = messageTemplateDao;
        this.cronTaskService = cronTaskService;
        this.xxlJobUtils = xxlJobUtils;
        this.cache = cache;
    }

    @Override
    public Page<MessageTemplate> queryList(MessageTemplateParam param) {
        PageRequest pageRequest = PageRequest.of(param.getPage() - 1, param.getPerPage());
//        String creator = CharSequenceUtil.isBlank(param.getCreator()) ? AustinConstant.DEFAULT_CREATOR : param.getCreator();
        return messageTemplateDao.findAll((Specification<MessageTemplate>) (root, query, cb) -> {
            List<Predicate> predicateList = new ArrayList<>();
            // 加搜索条件
            if (CharSequenceUtil.isNotBlank(param.getKeywords())) {
                predicateList.add(cb.like(root.get("name").as(String.class), "%" + param.getKeywords() + "%"));
            }
            predicateList.add(cb.equal(root.get("isDeleted").as(Integer.class), CommonConstant.FALSE));
//            predicateList.add(cb.equal(root.get("creator").as(String.class), creator));
            Predicate[] p = new Predicate[predicateList.size()];
            // 查询
            query.where(cb.and(predicateList.toArray(p)));
            // 排序
            query.orderBy(cb.desc(root.get("updated")));
            return query.getRestriction();
        }, pageRequest);
    }

    @Override
    public Long count() {
        return messageTemplateDao.countByIsDeletedEquals(CommonConstant.FALSE);
    }

    @Override
    public MessageTemplate saveOrUpdate(MessageTemplate messageTemplate) {
        if (Objects.isNull(messageTemplate.getId())) {
            initStatus(messageTemplate);
        } else {
            resetStatus(messageTemplate);
        }

        messageTemplate.setUpdated(Math.toIntExact(DateUtil.currentSeconds()));

        var updated = messageTemplateDao.save(messageTemplate);

        cache.removeMessageTemplate(updated.getId());
        return updated;
    }


    @Override
    public void deleteByIds(List<Long> ids) {
        Iterable<MessageTemplate> messageTemplates = messageTemplateDao.findAllById(ids);
        messageTemplates.forEach(messageTemplate -> messageTemplate.setIsDeleted(CommonConstant.TRUE));
        for (MessageTemplate messageTemplate : messageTemplates) {
            if (Objects.nonNull(messageTemplate.getCronTaskId()) && messageTemplate.getCronTaskId() > 0) {
                cronTaskService.deleteCronTask(messageTemplate.getCronTaskId());
            }
        }
        messageTemplateDao.saveAll(messageTemplates);
        cache.removeMessageTemplate(ids);
    }

    @Override
    public MessageTemplate queryById(Long id) {
        return cache.getMessageTemplate(id).orElse(null);
    }

    @Override
    public List<MessageTemplate> queryByIds(Long[] ids) {
        var res = messageTemplateDao.findAllById(List.of(ids));
        res.forEach(cache::put);

        return res;
    }

    @Override
    public void copy(Long id) {
        MessageTemplate messageTemplate = cache.getMessageTemplate(id).orElse(null);
        if (Objects.nonNull(messageTemplate)) {
            MessageTemplate clone = ObjectUtil.clone(messageTemplate).setId(null).setCronTaskId(null);
            MessageTemplate saved = messageTemplateDao.save(clone);
            cache.put(saved);
        }
    }

    @Override
    public Set<String> getTestContent(String msgContent) {
        return getPlaceholderList(msgContent);
    }

    @Override
    public BasicResultVO<Void> startCronTask(Long id) {
        // 1.获取消息模板的信息
        MessageTemplate messageTemplate = messageTemplateDao.findById(id).orElse(null);
        if (Objects.isNull(messageTemplate)) {
            return BasicResultVO.fail();
        }

        // 2.动态创建或更新定时任务
        XxlJobInfo xxlJobInfo = xxlJobUtils.buildXxlJobInfo(messageTemplate);

        // 3.获取taskId(如果本身存在则复用原有任务，如果不存在则得到新建后任务ID)
        Integer taskId = messageTemplate.getCronTaskId();
        BasicResultVO<Integer> basicResultVO = cronTaskService.saveCronTask(xxlJobInfo);
        if (Objects.isNull(taskId) && RespStatusEnum.SUCCESS.getCode().equals(basicResultVO.getStatus()) && Objects.nonNull(basicResultVO.getData())) {
            taskId = basicResultVO.getData();
        }

        // 4. 启动定时任务
        if (Objects.nonNull(taskId)) {
            cronTaskService.startCronTask(taskId);
            MessageTemplate clone = ObjectUtil.clone(messageTemplate).setMsgStatus(MessageStatus.RUN.getCode()).setCronTaskId(taskId).setUpdated(Math.toIntExact(DateUtil.currentSeconds()));
            messageTemplateDao.save(clone);
            return BasicResultVO.success();
        }
        return BasicResultVO.fail();
    }

    @Override
    public BasicResultVO<Void> stopCronTask(Long id) {
        // 1.修改模板状态
        MessageTemplate messageTemplate = cache.getMessageTemplate(id).orElse(null);
        if (Objects.isNull(messageTemplate)) {
            return BasicResultVO.fail();
        }
        MessageTemplate clone = ObjectUtil.clone(messageTemplate).setMsgStatus(MessageStatus.STOP.getCode()).setUpdated(Math.toIntExact(DateUtil.currentSeconds()));
        messageTemplateDao.save(clone);
        cache.removeMessageTemplate(id);

        // 2.暂停定时任务
        return cronTaskService.stopCronTask(clone.getCronTaskId());
    }


    @Override
    public BasicResultVO<Void> startAllCronTask() {
        List<MessageTemplate> templates = messageTemplateDao.findAll()
                .stream()
                .filter(t -> t.getMsgStatus().equals(20))
                .toList();

        List<Future<BasicResultVO<Void>>> futures = new ArrayList<>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (MessageTemplate messageTemplate : templates) {
                var future = executor.submit(() -> startCronTask(messageTemplate.getId()));
                futures.add(future);
            }
        }

        long count = futures.stream()
                .map(f -> {
                    try {
                        return f.get();
                    } catch (Exception e) {
                        log.error("startAllCronTask fail!", e);
                        return BasicResultVO.fail();
                    }
                })
                .filter(res -> !RespStatusEnum.SUCCESS.getCode().equals(res.getStatus()))
                .count();


        if (count > 0) {
            return BasicResultVO.fail();
        }
        return BasicResultVO.success();
    }

    @Override
    public BasicResultVO<Void> stopAllCronTask() {
        List<MessageTemplate> templates = messageTemplateDao.findAll()
                .stream()
                .filter(t -> t.getMsgStatus().equals(30))
                .toList();

        List<Future<BasicResultVO<Void>>> futures = new ArrayList<>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (MessageTemplate messageTemplate : templates) {
                var future = executor.submit(() -> stopCronTask(messageTemplate.getId()));
                futures.add(future);
            }
        }
        long count = futures.stream()
                .map(f -> {
                    try {
                        return f.get();
                    } catch (Exception e) {
                        log.error("stopAllCronTask fail!", e);
                        return BasicResultVO.fail();
                    }
                })
                .filter(res -> !RespStatusEnum.SUCCESS.getCode().equals(res.getStatus()))
                .count();

        if (count > 0) {
            return BasicResultVO.fail();
        }
        return BasicResultVO.success();
    }


    /**
     * 初始化状态信息
     *
     * @param messageTemplate
     */
    private void initStatus(MessageTemplate messageTemplate) {
        messageTemplate.setFlowId(CharSequenceUtil.EMPTY)
                .setMsgStatus(MessageStatus.INIT.getCode()).setAuditStatus(AuditStatus.WAIT_AUDIT.getCode())
                .setCreator(CharSequenceUtil.isBlank(messageTemplate.getCreator()) ? AustinConstant.DEFAULT_CREATOR : messageTemplate.getCreator())
                .setUpdator(CharSequenceUtil.isBlank(messageTemplate.getUpdator()) ? AustinConstant.DEFAULT_UPDATOR : messageTemplate.getUpdator())
                .setTeam(CharSequenceUtil.isBlank(messageTemplate.getTeam()) ? AustinConstant.DEFAULT_TEAM : messageTemplate.getTeam())
                .setAuditor(CharSequenceUtil.isBlank(messageTemplate.getAuditor()) ? AustinConstant.DEFAULT_AUDITOR : messageTemplate.getAuditor())
                .setCreated(Math.toIntExact(DateUtil.currentSeconds()))
                .setIsDeleted(CommonConstant.FALSE);

    }

    /**
     * 1. 重置模板的状态
     * 2. 修改定时任务信息(如果存在)
     *
     * @param messageTemplate
     */
    private void resetStatus(MessageTemplate messageTemplate) {
        messageTemplate.setUpdator(messageTemplate.getUpdator())
                .setMsgStatus(MessageStatus.INIT.getCode()).setAuditStatus(AuditStatus.WAIT_AUDIT.getCode());

        // 从数据库查询并注入 定时任务 ID
        MessageTemplate dbMsg = queryById(messageTemplate.getId());
        if (Objects.nonNull(dbMsg) && Objects.nonNull(dbMsg.getCronTaskId())) {
            messageTemplate.setCronTaskId(dbMsg.getCronTaskId());
        }

        if (Objects.nonNull(messageTemplate.getCronTaskId()) && TemplateType.CLOCKING.getCode().equals(messageTemplate.getTemplateType())) {
            XxlJobInfo xxlJobInfo = xxlJobUtils.buildXxlJobInfo(messageTemplate);
            cronTaskService.saveCronTask(xxlJobInfo);
            cronTaskService.stopCronTask(messageTemplate.getCronTaskId());
        }
    }

    private Set<String> getPlaceholderList(String content) {

        // 内容为空，直接返回
        if (content == null || content.isEmpty()) {
            return Collections.emptySet();
        }
        
        int ignore_tg = 0;
        int start_tg = 1;
        int read_tg = 2;

        StringBuilder sb = new StringBuilder();
        Set<String> placeholderSet = new HashSet<>();
        int modeTg = ignore_tg;

        for (char c : content.toCharArray()) {
            switch (c) {
                case '{':
                    if (modeTg == ignore_tg) {
                        sb.append(c);
                        modeTg = start_tg;
                    }
                    break;
                case '$':
                    if (modeTg == start_tg) {
                        sb.append(c);
                        modeTg = read_tg;
                    } else {
                        sb.setLength(0);
                        modeTg = ignore_tg;
                    }
                    break;
                case '}':
                    if (modeTg == read_tg) {
                        sb.append(c);
                        String placeholder = sb.toString();
                        placeholderSet.add(placeholder.replaceAll("[\\{\\$\\}]", ""));
                        sb.setLength(0);
                        modeTg = ignore_tg;
                    } else if (modeTg == start_tg) {
                        sb.setLength(0);
                        modeTg = ignore_tg;
                    }
                    break;
                default:
                    if (modeTg == read_tg) {
                        sb.append(c);
                    } else if (modeTg == start_tg) {
                        sb.setLength(0);
                        modeTg = ignore_tg;
                    }
                    break;
            }
        }

        return placeholderSet;
    }

}
