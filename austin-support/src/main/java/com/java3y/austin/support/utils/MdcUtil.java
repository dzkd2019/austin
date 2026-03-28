package com.java3y.austin.support.utils;

import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.Callable;

public class MdcUtil {
    public static Runnable wrap(Map<String, String> mdcContext, Runnable runnable) {
        return () -> {
            try {
                if(mdcContext == null) runnable.run();
                MDC.setContextMap(mdcContext);
                runnable.run();
            } finally {
                MDC.clear();
            }
        };
    }

    public static <T> Callable<T> wrap(Map<String, String> mdcContext, Callable<T> callable) {
        return () -> {
            try {
                if(mdcContext == null) return callable.call();
                MDC.setContextMap(mdcContext);
                return callable.call();
            } finally {
                MDC.clear();
            }
        };
    }
}
