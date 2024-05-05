package com.sakame;

import cn.hutool.core.io.FileUtil;
import com.sakame.model.KeyValue;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 用于输出正确答案，一次性读取所有文件并输出
 * 而在 mr 中所有文件的计算会由多个 worker 进程完成
 * @author sakame
 * @version 1.0
 */
public class Sequential {
    public static void main(String[] args) {
        Object[] objects = loadPlugin("WordCount");
        Method map = (Method)objects[0];
        Method reduce = (Method)objects[1];

        // 读取所有文件并调用 map 函数
        List<KeyValue> intermediate = new ArrayList<>();
        String userDir = System.getProperty("user.dir");
        String path = userDir + "\\map-reduce\\src\\main\\resources\\";
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(path), "pg-*.txt")) {
            for (Path file: stream) {
                String fileName = file.getParent().toString() + File.separator + file.getFileName().toString();
                FileReader fileReader = new FileReader(fileName);
                BufferedReader bufferedReader = new BufferedReader(fileReader);

                // 读取文件所有内容
                StringBuilder stringBuilder = new StringBuilder();
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    stringBuilder.append(line);
                }

                List<KeyValue> kva = (List) map.invoke(objects[2], fileName, stringBuilder.toString());
                for (KeyValue kv: kva) {
                    intermediate.add(kv);
                }

                bufferedReader.close();
                fileReader.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 对 kv 进行排序
        intermediate = intermediate.stream()
                .sorted(Comparator.comparing(KeyValue::getKey))
                .collect(Collectors.toList());

        // 文件位置
        String mrDir = userDir + "\\map-reduce\\tmp";
        if (!FileUtil.exist(mrDir)) {
            FileUtil.mkdir(mrDir);
        }
        String ofile = mrDir + File.separator + "mr-out-0.txt";

        // 调用 reduce 函数并写入内容到文件
        try {
            FileWriter fileWriter = new FileWriter(ofile);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            for (int i = 0; i < intermediate.size();) {
                // 记录 key 相同的 value 范围
                int j = i + 1;
                while (j < intermediate.size()
                        && intermediate.get(j).getKey().equals(intermediate.get(i).getKey())) {
                    j++;
                }
                List<String> values = new ArrayList<>();
                for (int k = i; k < j; k++) {
                    values.add(intermediate.get(k).getValue());
                }

                // 获取单个 key 对应的输出
                String output = (String) reduce.invoke(objects[2], intermediate.get(i).getKey(), values);
                bufferedWriter.write(String.format("%s %s\n", intermediate.get(i).getKey(), output));
                i = j;
            }
            bufferedWriter.close();
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据类名加载对应的 map 和 reduce 函数
     * @param fileName
     * @return
     */
    public static Object[] loadPlugin(String fileName) {
        String className = "com.sakame.apps." + fileName;
        try {
            Class<?> plugin = Class.forName(className);
            Method map = plugin.getMethod("map", String.class, String.class);
            Method reduce = plugin.getMethod("reduce", String.class, List.class);
            return new Object[]{map, reduce, plugin.newInstance()};
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
