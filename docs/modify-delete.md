# 资源修改、删除操作

# 简介
对文件列表进行修改生命周期、修改存储类型、修改状态、删除操作。

### 配置文件选项
```
process=lifecycle/type/status/delete
process-batch=true
type=1
status=0
days=0
```
`process=lifecycle/type/status/delete` 表示更新生命周期/修改存储类型/修改状态/删除操作  
`process-batch=true` 表示是否启用 batch 操作  
`type=` 0 表示标准存储，1 表示低频存储  
`status=` 0表示文件启用，1 表示文件禁用  
`days=0` 单位天数，为 0 时表示永久的生命周期  

### 命令行方式
```
-process=lifecycle -process-batch=true -days=0  
-process=type -process-batch=true -type=  
-process=status -process-batch=true -status=  
-process=delete -process-batch=true  
```
