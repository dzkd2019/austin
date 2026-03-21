package com.java3y.austin.common.pipeline;

import java.util.List;

/**
 * 业务执行模板（把责任链的逻辑串起来）
 *
 * @author 3y
 */
public class ProcessTemplate<T extends ProcessModel> {

    private List<BusinessProcess<T>> processList;

    public List<BusinessProcess<T>> getProcessList() {
        return processList;
    }

    public void setProcessList(List<BusinessProcess<T>> processList) {
        this.processList = processList;
    }
}