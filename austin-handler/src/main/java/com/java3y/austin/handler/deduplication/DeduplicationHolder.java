package com.java3y.austin.handler.deduplication;

import com.java3y.austin.handler.deduplication.service.DeduplicationService;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;


/**
 * @author huskey
 * @date 2022/1/18
 */
@Service
public class DeduplicationHolder {

    private final Map<DeduplicationType, DeduplicationService> serviceHolder = new HashMap<>(4);


    public DeduplicationService selectService(DeduplicationType key) {
        return serviceHolder.get(key);
    }


    public void putService(DeduplicationType key, DeduplicationService service) {
        serviceHolder.put(key, service);
    }
}
