package com.java3y.austin.common.pipeline;


import com.java3y.austin.common.enums.RespStatusEnum;
import com.java3y.austin.common.vo.BasicResultVO;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 流程控制器
 *
 * @author 3y
 */
@Data
public class ProcessController {

    /**
     * 模板映射
     */
    private Map<String, ProcessTemplate<? extends ProcessModel>> templateConfig = null;


    /**
     * 执行责任链
     *
     * @return 返回上下文内容
     */
    @SuppressWarnings("unchecked")
    public <T extends ProcessModel> ProcessContext<T> process(ProcessContext<T> context) {

        /*
          前置检查
         */
        try {
            preCheck(context);
        } catch (ProcessException e) {
            return (ProcessContext<T>) e.getProcessContext();
        }

        /*
          遍历流程节点
         */
        ProcessTemplate<T> template = (ProcessTemplate<T>) templateConfig.get(context.getCode());
        List<BusinessProcess<T>> processList = template.getProcessList();
        for (BusinessProcess<T> businessProcess : processList) {
            businessProcess.process(context);
            if (Boolean.TRUE.equals(context.getNeedBreak())) {
                break;
            }
        }
        return context;
    }


    /**
     * 执行前检查，出错则抛出异常
     *
     * @param context 执行上下文
     * @throws ProcessException 异常信息
     */
    @SuppressWarnings("unchecked")
    private <T extends ProcessModel> void preCheck(ProcessContext<T> context) throws ProcessException {
        // 上下文
        if (Objects.isNull(context)) {
            context = new ProcessContext<>();
            context.setResponse(BasicResultVO.fail(RespStatusEnum.CONTEXT_IS_NULL));
            throw new ProcessException(context);
        }

        // 业务代码
        String businessCode = context.getCode();
        if (Objects.isNull(businessCode)) {
            context.setResponse(BasicResultVO.fail(RespStatusEnum.BUSINESS_CODE_IS_NULL));
            throw new ProcessException(context);
        }

        // 执行模板
        ProcessTemplate<T> processTemplate = (ProcessTemplate<T>) templateConfig.get(businessCode);
        if (Objects.isNull(processTemplate)) {
            context.setResponse(BasicResultVO.fail(RespStatusEnum.PROCESS_TEMPLATE_IS_NULL));
            throw new ProcessException(context);
        }

        // 执行模板列表
        List<BusinessProcess<T>> processList = processTemplate.getProcessList();
        if (Objects.isNull(processList) || processList.isEmpty()) {
            context.setResponse(BasicResultVO.fail(RespStatusEnum.PROCESS_LIST_IS_NULL));
            throw new ProcessException(context);
        }

    }


}
