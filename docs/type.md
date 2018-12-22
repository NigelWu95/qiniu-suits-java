# 资源存储类型更新操作

# 简介
对空间中的资源进行修改存储类型。

### 配置文件选项
```
process=type
process-batch=true
type=1
```
`process=type` 表示修改存储类型操作  
`process-batch=true` 表示是否启用 batch 操作（默认开启）  
`type=` 0 表示标准存储，1 表示低频存储  

### 命令行方式
```
-process=type -process-batch=true -type=  
```
