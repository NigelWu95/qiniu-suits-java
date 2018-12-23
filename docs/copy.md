# 资源复制

# 简介
对空间中的资源进行**复制**到另一个目标空间。

### 配置文件选项
```
process=copy
process-ak=
process-sk=
bucket=
to-bucket=
keep-key=true
add-prefix=
```
`process=copy` 表示复制操作  
`process-ak` 目标账号的 ak，未设置时使用 ak 参数的值  
`process-sk` 目标账号的 sk，未设置时使用 sk 参数的值  
`bucket` 操作的资源所在空间（数据源为 list 时无需设置）  
`to-bucket` 目标空间  
`keep-key` 表示是否维持原文件名  
`add-prefix=` 表示复制之后的文件名添加指定前缀  

### 命令行方式
```
-process=copy -process-ak= -process-sk= -bucket= -to-bucket= -keep-key= -add-prefix=
```
