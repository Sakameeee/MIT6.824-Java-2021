# MIT6.824-Java-2021
***
因为使用的是自己实现的 rpc，所以跳过了关于 reliable 的测试  
由于 junit 测试多个函数时并不是真正的关闭虚拟机而后重启，因此如果直接运行整个 test 文件会加大出现 bug 的概率

+ lab1：基于 windows 本地文件系统实现 MapReduce  
+ lab2：实现了 raft 库，包含选举，日志复制，持久化，快照等功能  
  + 关于 2c 的测试有极低概率会出现 bug，原因是 raft 重置了定时器之后立马又开始了选举，目前解决不了（悲
  + 关于 2d 的测试有极低概率出现日志不会提交的情况
+ lab3：组合上层 kv 存储和下层 raft 库，实现了带复制和快照生成的 kv 系统
  + 关于 3a 的速度测试无法通过，单次操作耗时大概是测试要求的 8 倍左右（逃
  + 关于 3b 的多客户端测试无法通过，因为 vertx 服务器不支持强制关闭服务器，无法模拟 crash 的情况。多客户端情况下可能出现 leader KVSerer 正在处理来自 client 的请求，而此时其他 server 正常关闭，leader 会一直处于等待当中无法释放线程从而导致后续的重启无法正常进行

## 依赖
***
本项目使用 `etcd` 实现了 rpc 远程调用，需要先下载 `etcd` 并运行  
> https://github.com/etcd-io/etcd/releases/  

*请确保在运行下面所有测试前先启动 `etcd`*

## Map Reduce
***
1. 运行 `Sequential` 的 main 函数生成正确的数据，路径为 `map-reduce/tmp/mr-correct-wc-txt`  
2. 运行 `Coordinator` 的 main 函数启动服务器  
3. 运行 `Worker` 的 main 函数，启动一个 `Worker` 进程（可以运行多个进程），此时系统会开始运行 map reduce 任务，生成的文件和第一步生成的文件在同一个目录下，当全部任务完成之后所有的进程都会自动结束  
4. 运行 test 目录下的 `MapReduceTest` 的测试函数，检验所有输出文件的内容和正确答案是否一直

## Raft
***
1. 所有的测试都在 test 目录的 `RaftTest` 文件里  

## KV Raft
***
1. 所有的测试都在 test 目录的 `KVRaftTest` 文件里  

