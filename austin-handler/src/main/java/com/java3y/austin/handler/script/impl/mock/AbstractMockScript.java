package com.java3y.austin.handler.script.impl.mock;

import com.java3y.austin.handler.script.SmsScript;
import com.java3y.austin.handler.script.impl.mock.utils.MockStatusConstant;
import jakarta.annotation.PostConstruct;
import com.java3y.austin.support.utils.WeightedRandomUtils;

public abstract class AbstractMockScript implements SmsScript {
    private static final WeightedRandomUtils<String> WEIGHTED_RANDOM_UTILS = new WeightedRandomUtils<>();

    @PostConstruct
    public void init() {
        WEIGHTED_RANDOM_UTILS.add(95, MockStatusConstant.SUCCESS);
        WEIGHTED_RANDOM_UTILS.add(3, MockStatusConstant.TIMEOUT);
        WEIGHTED_RANDOM_UTILS.add(1, MockStatusConstant.FAILURE);
        WEIGHTED_RANDOM_UTILS.add(1, MockStatusConstant.ERROR);
    }

    protected String next() {
        return WEIGHTED_RANDOM_UTILS.next();
    }
}
