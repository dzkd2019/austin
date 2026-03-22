package com.java3y.austin.common.pipeline;

import com.java3y.austin.common.enums.RespStatusEnum;
import lombok.Getter;

import java.util.Objects;

/**
 * @author SamLee
 * @since 2022-03-29
 */
@Getter
public class ProcessException extends RuntimeException {

    /**
     * 流程处理上下文
     */
    private final ProcessContext<? extends ProcessModel> processContext;

    public ProcessException(ProcessContext<? extends ProcessModel> processContext) {
        super();
        this.processContext = processContext;
    }

    public ProcessException(ProcessContext<? extends ProcessModel> processContext, Throwable cause) {
        super(cause);
        this.processContext = processContext;
    }

    @Override
    public String getMessage() {
        if (Objects.nonNull(this.processContext)) {
            return this.processContext.getResponse().getMsg();
        }
        return RespStatusEnum.CONTEXT_IS_NULL.getMsg();

    }

}
