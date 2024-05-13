package com.sakame;

import com.sakame.model.KeyValue;
import org.junit.Assert;
import org.junit.Test;
import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author sakame
 * @version 1.0
 */
public class MapReduceTest {

    private static final String GLOBAL_DIR = System.getProperty("user.dir") + "\\tmp";

    @Test
    public void checkAllFiles() throws Exception {
        List<KeyValue> list1 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String fileName = String.format("mr-out-%d.txt", i);
            FileReader fileReader = new FileReader(GLOBAL_DIR + File.separator + fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] split = line.split(" ");
                list1.add(new KeyValue(split[0], split[1]));
            }

            bufferedReader.close();
            fileReader.close();
        }

        list1 = list1.stream()
                .sorted(Comparator.comparing(KeyValue::getKey))
                .collect(Collectors.toList());

        List<KeyValue> list2 = new ArrayList<>();
        String fileName = "mr-correct-wc.txt";
        FileReader fileReader = new FileReader(GLOBAL_DIR + File.separator + fileName);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            String[] split = line.split(" ");
            list2.add(new KeyValue(split[0], split[1]));
        }

        bufferedReader.close();
        fileReader.close();

        Assert.assertArrayEquals(list1.toArray(), list2.toArray());
    }
}