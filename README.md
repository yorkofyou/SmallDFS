├── master.go // 调用Master.go启动DFS的Client

├── client.go // 调用Clientgo启动DFS的Client

├── Client1 // Client1的工作目录

│ └── achunkhashs // Client的工作目录下存储文件块数据哈希值的目录

├── Client2 // Client2的工作目录

│ └── chunkhashs

├── Client3 // Client3的工作目录

│ └── chunkhashs

├── Client4 // Client4的工作目录

│ └── chunkhashs

└── src // dfs源码，需将dfs目录拷贝到$GOPATH/src下面让整个Go工程跑起来

 └── dfs
 
  ├── master.go // master相关的所有操作
  
  ├── client.go // client相关的所有操作
  
  ├── config.go // 系统的所有数据结构定义、参数相关
  
  └── util.go // 文件操作的一些工具函数

运行分布式文件系统SmallDFS

1. 分别运行各个Client，打开终端，跳转到SmallDFS目录下

   ```bash
   # Terminal 1
   go run client.go -which 1
   # Terminal 2
   go run client.go -which 2
   # Terminal 3
   go run client.go -which 3
   # Terminal 4
   go run client.go -which 4
   ```

2. 运行Master，打开终端，跳转到SmallDFS目录下

   ```bash
   # Terminal 5
   go run master.go
   ```

3. 运行User，打开终端，跳转到SmallDFS目录下

   ```bash
   # Put
   go run user.go -which 1 -put "a.txt"
   # Get
   go run user.go -which 2 -get "a.txt"
   ```

