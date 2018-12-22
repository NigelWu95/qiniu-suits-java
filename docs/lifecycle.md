# 资源生命周期更新操作

# 简介
对空间中的资源修改生命周期。

### 配置文件选项
```
process=lifecycle
process-batch=true
days=0
```
`process=lifecycle` 表示更新生命周期操作  
`process-batch=true` 表示是否启用 batch 操作（默认开启）  
`days=0` 单位天数，为 0 时表示永久的生命周期  

### 命令行方式
```
-process=lifecycle -process-batch=true -days=0  
```
