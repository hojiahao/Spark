package cn.edu.szu.dataskew.dataproduce;

import java.io.Serializable;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

public class ZipfGenerator implements Serializable {
    private final Random random = new Random(0);
    public NavigableMap<Double, Integer> map;
    private static final double constant = 1.0;

    public ZipfGenerator(int nums, double skewness) {
        // create the TreeMap
        map = computeMap(nums, skewness);
    }

    private static NavigableMap<Double, Integer> computeMap(int size, double skewness) {
        NavigableMap<Double, Integer> map = new TreeMap<Double, Integer>();
        // 总频率
        double frequency = 0;
        // 对于每个rank，计算对应的词频，计算总词频
        for (int i = 1; i <= size; i++) {
            // the frequency in position i
            frequency += (constant / Math.pow(i, skewness));
        }
        // 计算每个rank对应的y值，所以靠前rank的y值区间远比后面rank的y值区间大
        double sum = 0;
        for (int i = 1; i <= size; i++) {
            double p = (constant / Math.pow(i, skewness)) / frequency;
            sum += p;
            map.put(sum, i - 1);
        }
        return map;
    }

    public int next() {
        double value = random.nextDouble();
        // 找最近y值对应的rank
        return map.ceilingEntry(value).getValue() + 1;
    }
}
