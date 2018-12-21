# 资源复制/移动/重命名

# 简介
对文件列表进行**复制/移动**到目标空间或者在当前空间中进行**重命名**。

### 配置文件选项
```
process=copy/move/rename
process-batch=true
process-ak=
process-sk=
to-bucket=bucket2
keep-key=true
add-prefix=
```
`process=copy/move/rename` 表示 copy/move/rename 操作  
`process-batch=true` 表示是否启用 batch 操作  
`process-ak=` 目标账号的 ak，未设置时使用 ak 参数的值  
`process-sk=` 目标账号的 sk，未设置时使用 sk 参数的值  
`to-bucket=bucket2` 目标空间  
`keep-key=true` 表示是否维持原文件名  
`add-prefix=` 表示复制之后的文件名添加指定前缀  

### 命令行方式
```
-process=copy/move/rename -process-batch=true -process-ak= -process-sk= -to-bucket=bucket2 -keep-key=true -add-prefix=
```

### 备注
copy 操作可以设置所有参数，move 和 rename 操作无需设置 keep-key，另外 rename 操作是
针对文件数据源输入的情况，需要设置每一个文件名对应的修改之后的文件名，下标参数为
newKey-index
