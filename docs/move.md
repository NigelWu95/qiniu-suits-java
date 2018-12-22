# 资源移动

# 简介
对空间中的资源进行**移动**到另一目标空间。

### 配置文件选项
```
process=move
process-batch=true
process-ak=
process-sk=
bucket=
to-bucket=
keep-key=true
add-prefix=
```
`process=move` 表示移动操作  
`process-batch` 表示是否启用 batch 操作（默认开启）  
`process-ak` 目标账号的 ak，未设置时使用 ak 参数的值  
`process-sk` 目标账号的 sk，未设置时使用 sk 参数的值  
`bucket` 操作的资源所在空间（数据源为 list 时无需设置）  
`to-bucket` 目标空间  
`keep-key` 表示是否维持原文件名  
`add-prefix=` 表示移动之后的文件名添加指定前缀  

### 命令行方式
```
-process=move -process-batch=true -process-ak= -process-sk= -bucket= -to-bucket= -keep-key= -add-prefix=
```
