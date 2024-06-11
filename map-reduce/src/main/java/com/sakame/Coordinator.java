package com.sakame;

import com.sakame.config.RegistryConfig;
import com.sakame.config.RpcConfig;
import com.sakame.constant.MasterStatus;
import com.sakame.constant.RpcConstant;
import com.sakame.constant.TaskStatus;
import com.sakame.constant.TaskType;
import com.sakame.model.CoordinatorStatus;
import com.sakame.model.ServiceMetaInfo;
import com.sakame.model.Task;
import com.sakame.model.dto.FinishTaskRequest;
import com.sakame.model.dto.FinishTaskResponse;
import com.sakame.model.dto.GetTaskRequest;
import com.sakame.model.dto.GetTaskResponse;
import com.sakame.registry.Registry;
import com.sakame.registry.RegistryFactory;
import com.sakame.server.HttpServer;
import com.sakame.server.VertxHttpServer;
import com.sakame.service.CoordinatorService;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coordinator 类
 *
 * @author sakame
 * @version 1.0
 */
@Slf4j
public class Coordinator implements CoordinatorService {
    private static final String GLOBAL_DIR = System.getProperty("user.dir") + "\\map-reduce\\tmp";
    private static volatile CoordinatorStatus status = new CoordinatorStatus();

    public static void main(String[] args) {
        String userDir = System.getProperty("user.dir");
        String path = userDir + "\\map-reduce\\src\\main\\resources\\";
        List<String> fileList = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(path), "pg-*.txt")) {
            for (Path file : stream) {
                fileList.add(file.getParent().toString() + File.separator + file.getFileName().toString());
            }
            Coordinator coordinator = makeCoordinator(fileList, 10);
            while (!coordinator.done()) {
                Thread.sleep(1000);
            }
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化 coordinator 状态
     *
     * @param files
     * @param nReduce
     */
    private static void init(List<String> files, int nReduce) {
        log.info("init coordinator, make map tasks");
        int n = files.size();
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Task task = new Task();
            task.setTaskId(i);
            task.setFiles(files);
            task.setStatus(TaskStatus.IDLE);
            tasks.add(task);
        }

        status.setTasks(tasks);
        status.setStatus(MasterStatus.MAP_PHASE);
        status.setNReduce(nReduce);
        status.setMapCount(n);
        status.setReentrantLock(new ReentrantLock());
    }

    /**
     * 创建 Coordinator 实例
     *
     * @param files
     * @param nReduce
     * @return
     */
    public static Coordinator makeCoordinator(List<String> files, int nReduce) {
        Coordinator c = new Coordinator();

        init(files, nReduce);

        c.server(c);
        return c;
    }

    /**
     * rpc handler，获取任务（消费端调用）
     *
     * @param getTaskRequest
     * @return
     */
    @Override
    public GetTaskResponse getTask(GetTaskRequest getTaskRequest) {
        status.getReentrantLock().lock();

        if (isAllFinish()) {
            nextPhase();
        }

        GetTaskResponse getTaskResponse = new GetTaskResponse();
        List<Task> tasks = status.getTasks();
        for (int i = 0; i < tasks.size(); i++) {
            Task task = tasks.get(i);
            getTaskResponse.setTaskId(i);
            if (task.getStatus() == TaskStatus.IDLE) {
                log.info("send task {} to worker", i);
                if (status.getStatus() == MasterStatus.MAP_PHASE) {
                    // 在 map 任务里有 mapCount 个任务，每个任务的文件数组都是一样的，大小为 mapCount
                    // worker 只需要一个文件就能完成任务
                    getTaskResponse.setFileNames(Arrays.asList(task.getFiles().get(i)));
                    getTaskResponse.setType(TaskType.MAP);
                    getTaskResponse.setNReduce(status.getNReduce());
                } else if (status.getStatus() == MasterStatus.REDUCE_PHASE) {
                    // 在 reduce 任务中有 nReduce 个任务，每个任务的文件数组大小都为 mapCount
                    // worker 需要所有桶编号相同的文件才能完成任务
                    getTaskResponse.setFileNames(task.getFiles());
                    getTaskResponse.setType(TaskType.REDUCE);
                    getTaskResponse.setNReduce(0);
                }
                task.setStartTime(LocalDateTime.now());
                task.setStatus(TaskStatus.IN_PROGRESS);
                status.getReentrantLock().unlock();
                return getTaskResponse;
            } else if (task.getStatus() == TaskStatus.IN_PROGRESS) {
                if (task.getStartTime().plus(Duration.ofSeconds(10)).isBefore(LocalDateTime.now())) {
                    log.info("resend task {} to worker", i);
                    if (status.getStatus() == MasterStatus.MAP_PHASE) {
                        getTaskResponse.setFileNames(Arrays.asList(task.getFiles().get(i)));
                        getTaskResponse.setType(TaskType.MAP);
                        getTaskResponse.setNReduce(status.getNReduce());
                    } else if (status.getStatus() == MasterStatus.REDUCE_PHASE) {
                        getTaskResponse.setFileNames(task.getFiles());
                        getTaskResponse.setType(TaskType.REDUCE);
                        getTaskResponse.setNReduce(0);
                    }
                    task.setStartTime(LocalDateTime.now());
                    task.setStatus(TaskStatus.IN_PROGRESS);
                    status.getReentrantLock().unlock();
                    return getTaskResponse;
                }
            }
        }

        status.getReentrantLock().unlock();
        return null;
    }

    /**
     * rpc handler，提交完成任务（消费端调用）
     *
     * @param finishTaskRequest
     * @return
     */
    @Override
    public FinishTaskResponse finishTask(FinishTaskRequest finishTaskRequest) {
        status.getReentrantLock().lock();
        FinishTaskResponse reply = new FinishTaskResponse();

        if (finishTaskRequest.getTaskId() < 0 || finishTaskRequest.getTaskId() > status.getTasks().size()) {
            reply.setOK(false);
            status.getReentrantLock().unlock();
            return reply;
        }
        status.getTasks().get(finishTaskRequest.getTaskId()).setStatus(TaskStatus.COMPLETED);
        if (isAllFinish()) {
            nextPhase();
        }

        reply.setOK(true);
        status.getReentrantLock().unlock();
        return reply;
    }

    /**
     * 启动 vertx 服务器，注册服务
     */
    public void server(Coordinator coordinator) {
        RpcConfig rpcConfig = RpcConfig.getRpcConfig();

        String serviceName = CoordinatorService.class.getName();

        RegistryConfig registryConfig = rpcConfig.getRegistryConfig();
        Registry registry = RegistryFactory.getInstance(registryConfig.getRegistry());
        ServiceMetaInfo serviceMetaInfo = new ServiceMetaInfo();
        serviceMetaInfo.setServiceName(serviceName);
        serviceMetaInfo.setServiceVersion(RpcConstant.DEFAULT_SERVICE_VERSION);
        serviceMetaInfo.setServiceHost(rpcConfig.getServerHost());
        serviceMetaInfo.setServicePort(2345);
        try {
            registry.register(serviceMetaInfo);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        HttpServer httpServer = new VertxHttpServer(coordinator);
        httpServer.doStart(2345);
    }

    /**
     * 所有 map 任务均完成调用，重新生成 tasks 数组用于 reduce
     */
    public void makeReduceTasks() {
        log.info("make reduce task");
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < status.getNReduce(); i++) {
            Task task = new Task();
            task.setTaskId(i);
            task.setStatus(TaskStatus.IDLE);
            // mapCount 对应的是 taskId
            List<String> files = new ArrayList<>();
            for (int j = 0; j < status.getMapCount(); j++) {
                String fileName = GLOBAL_DIR + File.separator + String.format("mr-%d-%d.txt", j, i);
                files.add(fileName);
            }
            task.setFiles(files);
            tasks.add(task);
        }

        // 之前是 mapCount 个任务，每个任务的文件数组都是一样的
        // 现在是 nReduce 个任务，每个任务的文件是桶编号相同的文件
        status.setTasks(tasks);
    }

    /**
     * 检查是否所有 map 任务均完成
     *
     * @return
     */
    public boolean isAllFinish() {
        List<Task> tasks = status.getTasks();
        for (Task task : tasks) {
            if (task.getStatus() != TaskStatus.COMPLETED) {
                return false;
            }
        }
        return true;
    }

    /**
     * 进入下一个阶段
     */
    public void nextPhase() {
        if (status.getStatus() == MasterStatus.MAP_PHASE) {
            log.info("change to REDUCE_PHASE");
            makeReduceTasks();
            status.setStatus(MasterStatus.REDUCE_PHASE);
        } else if (status.getStatus() == MasterStatus.REDUCE_PHASE) {
            log.info("change to FINISH_PHASE");
            status.setStatus(MasterStatus.FINISH_PHASE);
        }
    }

    /**
     * 查询是否所有的 worker 都已完成任务
     *
     * @return
     */
    public boolean done() {
        status.getReentrantLock().lock();
        if (status.getStatus() == MasterStatus.FINISH_PHASE) {
            status.getReentrantLock().unlock();
            return true;
        }
        status.getReentrantLock().unlock();
        return false;
    }
}
