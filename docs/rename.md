# 资源重命名操作

# 简介
对空间中的资源进行**重命名**。

### 配置文件选项
```
process=rename
process-ak=
process-sk=
bucket=
add-prefix=
```
`process=rename` 表示重命名操作  
`process-ak` 目标账号的 ak，未设置时使用 ak 参数的值  
`process-sk` 目标账号的 sk，未设置时使用 sk 参数的值  
`bucket` 操作的资源所在空间（数据源为 list 时无需设置）  
`add-prefix` 表示重命名之后的文件名添加指定前缀（默认为 ""）  

### 命令行方式
```
-process=rename -process-ak= -process-sk= -bucket= -add-prefix=
```

### 备注
rename 操作是针对文件数据源输入的情况，需要设置每一个文件名对应的修改之后的文件名，
下标参数为 newKey-index
