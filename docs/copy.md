# 资源复制

# 简介
对文件列表进行复制到目标空间。

### 配置文件选项
```
process=copy
process-batch=true
process-ak=
process-sk=
to-bucket=bucket2
keep-key=true
add-prefix=
```
`process=copy` 表示 copy 操作  
`process-batch=true` 表示是否启用 batch 操作  
`process-ak=` 目标账号的 ak，未设置时使用 ak 参数的值  
`process-sk=` 目标账号的 sk，未设置时使用 sk 参数的值  
`to-bucket=bucket2` 目标空间  
`keep-key=true` 表示是否维持原文件名  
`add-prefix=` 表示复制之后的文件名添加指定前缀  

### 命令行方式
```
-process=copy -process-batch=true -process-ak= -process-sk= -to-bucket=bucket2 -keep-key=true -add-prefix=
```
