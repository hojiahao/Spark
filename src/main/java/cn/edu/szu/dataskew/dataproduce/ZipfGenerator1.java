package cn.edu.szu.dataskew.dataproduce;

import java.io.Serializable;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

public class ZipfGenerator1 implements Serializable {
    private final Random random = new Random(0);
    public NavigableMap<Double, String> map;
    private static final double constant = 1.0;
    private final String[] words;

    public ZipfGenerator1(String[] words, double skewness) {
        this.words = words;
        // create the TreeMap
        map = computeMap(words.length, skewness);
    }

    // size为rank个数，skewness为数据倾斜程度, 取值为0表示数据无倾斜，取值越大倾斜程度越高
    private NavigableMap<Double, String> computeMap(int size, double skewness) {
        NavigableMap<Double, String> map = new TreeMap<Double, String>();
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
            map.put(sum, words[i - 1]);
        }
        return map;
    }

    public String next() {
        // [1,n]
        double value = random.nextDouble();
        // 找最近y值对应的rank
        return map.ceilingEntry(value).getValue();
    }
}
