import cn.edu.szu.dataskew.dataproduce.ZipfGenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.NavigableMap;

public class Test {
    public static void main(String[] args) {
        ZipfGenerator z1 = new ZipfGenerator(100, 0.5);
        PrintWriter printWriter = null;
        try {
            printWriter = new PrintWriter(new FileWriter("inputdata/zip-f/zipf_100_0.5.txt"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (NavigableMap.Entry<Double, Integer> entry: z1.map.entrySet()) {
            // System.out.println("key: " + entry.getKey() + " value: " + entry.getValue());
            String str = "Key: " + entry.getKey() + ", Value: " + entry.getValue();
            printWriter.println(str);
        }
        printWriter.close();
    }
}
