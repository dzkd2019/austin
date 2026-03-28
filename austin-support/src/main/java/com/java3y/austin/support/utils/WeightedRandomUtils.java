package com.java3y.austin.support.utils;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

public class WeightedRandomUtils<T> {
    // 存储累加权重与对应的对象
    private final NavigableMap<Double, T> map = new TreeMap<>();
    // 总权重
    private double totalWeight = 0;

    /**
     * 添加权重对象
     * @param weight 权重值 (可以是 70, 10 这种百分比，也可以是 1000, 200 这种具体数量)
     * @param result 命中的结果
     */
    public WeightedRandomUtils<T> add(double weight, T result) {
        if (weight <= 0) {
            return this; // 忽略无效权重
        }
        totalWeight += weight;
        map.put(totalWeight, result);
        return this;
    }

    /**
     * 获取下一个随机元素
     */
    public T next() {
        if (map.isEmpty()) {
            return null;
        }
        // 生成一个 [0, totalWeight) 范围内的随机数
        // 注意：在虚拟线程/高并发环境下，推荐使用 ThreadLocalRandom
        double randomValue = ThreadLocalRandom.current().nextDouble() * totalWeight;

        // 返回第一个大于该随机数的 Entry 的值
        return map.higherEntry(randomValue).getValue();
    }
}
