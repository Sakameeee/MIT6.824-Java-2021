package com.sakame;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sakame.constant.TaskType;
import com.sakame.model.KeyValue;
import com.sakame.model.dto.FinishTaskRequest;
import com.sakame.model.dto.FinishTaskResponse;
import com.sakame.model.dto.GetTaskRequest;
import com.sakame.model.dto.GetTaskResponse;
import com.sakame.proxy.ServiceProxyFactory;
import com.sakame.service.CoordinatorService;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

/**
 * worker 类
 * @author sakame
 * @version 1.0
 */
@Slf4j
public class Worker {
    private static final Gson GSON = new Gson();

    private static final String GLOBAL_DIR = System.getProperty("user.dir") + "\\map-reduce\\tmp";

    public static void main(String[] args) {
        Object[] objects = loadPlugin("WordCount");
        new Worker().startWorker(objects);
        System.exit(0);
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

    /**
     * use ihash(key) % NReduce to choose the reduce
     * task number for each KeyValue emitted by Map.
     * @param key
     * @return
     */
    public int ihash(String key) {
        CRC32 crc = new CRC32();
        crc.update(key.getBytes(StandardCharsets.UTF_8));
        return (int) (crc.getValue() & 0x7fffffff);
    }

    /**
     * 启动一个 worker 进程
     * @param objects
     */
    public void startWorker(Object[] objects) {
        Method map = (Method)objects[0];
        Method reduce = (Method)objects[1];

        while (true) {
            GetTaskRequest getTaskRequest = new GetTaskRequest();
            System.out.println("get task request:" + getTaskRequest);
            GetTaskResponse getTaskResponse = callGetTask(getTaskRequest);
            System.out.println("receive task reply:" + getTaskResponse);

            if (getTaskResponse == null || getTaskResponse.getType() == TaskType.STOP) {
                return;
            }

            // 处理 map 函数
            switch (getTaskResponse.getType()) {
                case TaskType.MAP:
                    if (CollUtil.isEmpty(getTaskResponse.getFileNames())) {
                        // todo
                    }
                    doMap(map, getTaskResponse, objects[2]);
                    FinishTaskRequest finishTaskRequest = FinishTaskRequest.builder()
                            .taskId(getTaskResponse.getTaskId())
                            .type(TaskType.MAP)
                            .build();
                    System.out.println("finish request:" + finishTaskRequest);
                    callFinishTask(finishTaskRequest);
                    System.out.println("receive finish reply:");
                    break;
                case TaskType.REDUCE:
                    if (CollUtil.isEmpty(getTaskResponse.getFileNames())) {
                        // todo
                    }
                    doReduce(reduce, getTaskResponse, objects[2]);
                    FinishTaskRequest finishTaskRequest1 = FinishTaskRequest.builder()
                            .taskId(getTaskResponse.getTaskId())
                            .type(TaskType.REDUCE)
                            .build();
                    System.out.println("finish request:" + finishTaskRequest1);
                    callFinishTask(finishTaskRequest1);
                    System.out.println("receive finish reply:");
                    break;
                case TaskType.WAIT:
                    System.out.println("wait task");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                default:
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    break;
            }
        }
    }

    /**
     * 执行 map 任务
     * @param map
     * @param reply
     * @param instance
     */
    public void doMap(Method map, GetTaskResponse reply, Object instance) {
        String fileName = reply.getFileNames().get(0);
        int nReduce = reply.getNReduce();
        List<KeyValue> kva = new ArrayList<>();
        try {
            FileReader fileReader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            // 读取单个文件生成 kv 集合
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
            }

            kva = (List) map.invoke(instance, fileName, stringBuilder.toString());

            bufferedReader.close();
            fileReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 对 kv 进行排序
        kva = kva.stream()
                .sorted(Comparator.comparing(KeyValue::getKey))
                .collect(Collectors.toList());

        // 创建临时文件
        System.out.println("encode to json");
        List<File> files = new ArrayList<>();
        for (int i = 0; i < nReduce; i++) {
            try {
                File file = File.createTempFile("mr-tmp-", ".txt", new File(GLOBAL_DIR));
                file.deleteOnExit();
                files.add(file);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // 通过哈希函数确定一个文件位置并写入 json 字符串，nReduce == files.size() == 切片个数
        int index;
        int nextIndex = 0;
        int n = kva.size();
        for (int i = 0; i < n;) {
            index = ihash(kva.get(i).getKey()) % nReduce;
            try {
                FileWriter fileWriter = new FileWriter(files.get(index), true);
                BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
                while (i == 0 || nextIndex == index) {
                    bufferedWriter.write(GSON.toJson(kva.get(i)));
                    bufferedWriter.newLine();
                    i++;
                    if (i == n) {
                        break;
                    }
                    nextIndex = ihash(kva.get(i).getKey()) % nReduce;
                }
                bufferedWriter.close();
                fileWriter.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 重命名文件
        for (int i = 0; i < nReduce; i++) {
            String newName = String.format("mr-%d-%d.txt", reply.getTaskId(), i);
            FileUtil.rename(files.get(i), newName, false);
        }
    }

    /**
     * 执行 reduce 任务
     * @param reduce
     * @param reply
     * @param instance
     */
    public void doReduce(Method reduce, GetTaskResponse reply, Object instance) {
        List<String> fileNames = reply.getFileNames();
        // 从临时文件中读取 map 计算的 kv
        List<KeyValue> kva = new ArrayList<>();
        for (String fileName : fileNames) {
            if (!FileUtil.exist(fileName)) {
                continue;
            }
            try {
                FileReader fileReader = new FileReader(fileName);
                BufferedReader bufferedReader = new BufferedReader(fileReader);

                // 收集所有的 kv
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    kva.add(GSON.fromJson(line, new TypeToken<KeyValue>() {
                    }.getType()));
                }

                bufferedReader.close();
                fileReader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            // todo:
            boolean delete = new File(fileName).delete();
            if (!delete) {
                log.warn("fail to delete temp file:" + fileName);
            }
        }

        // 对 kv 进行排序
        kva = kva.stream()
                .sorted(Comparator.comparing(KeyValue::getKey))
                .collect(Collectors.toList());

        // 调用 reduce 函数处理 key 相同的 value 获取输出并写入内容到文件
        File file = null;
        try {
            file = File.createTempFile("mr-out-tmp-", ".txt", new File(GLOBAL_DIR));
            file.deleteOnExit();
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            for (int i = 0; i < kva.size();) {
                // 记录 key 相同的 value 范围
                int j = i + 1;
                while (j < kva.size()
                        && kva.get(j).getKey().equals(kva.get(i).getKey())) {
                    j++;
                }
                List<String> values = new ArrayList<>();
                for (int k = i; k < j; k++) {
                    values.add(kva.get(k).getValue());
                }

                // 获取单个 key 对应的输出
                String output = (String) reduce.invoke(instance, kva.get(i).getKey(), values);
                bufferedWriter.write(String.format("%s %s\n", kva.get(i).getKey(), output));
                i = j;
            }

            bufferedWriter.close();
            fileWriter.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // 重命名文件
        String newName = String.format("mr-out-%d.txt", reply.getTaskId());
        FileUtil.rename(file, newName, false);
    }

    /**
     * 通过 rpc 向 coordinator 请求分配任务
     * @param args
     * @return
     */
    public GetTaskResponse callGetTask(GetTaskRequest args) {
        CoordinatorService coordinatorService = ServiceProxyFactory.getProxy(CoordinatorService.class);
        return coordinatorService.getTask(args);
    }

    /**
     * 通过 rpc 向 coordinator 提交已完成任务
     * @param args
     * @return
     */
    public FinishTaskResponse callFinishTask(FinishTaskRequest args) {
        CoordinatorService coordinatorService = ServiceProxyFactory.getProxy(CoordinatorService.class);
        return coordinatorService.finishTask(args);
    }
}
