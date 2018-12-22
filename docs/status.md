# 资源状态更新操作

# 简介
对空间中的资源修改状态。

### 配置文件选项
```
process=status
process-batch=true
status=0
```
`process=status` 表示修改状态操作  
`process-batch=true` 表示是否启用 batch 操作（默认开启）  
`status=` 0表示文件启用，1 表示文件禁用  

### 命令行方式
```
-process=status -process-batch=true -status=  
```
