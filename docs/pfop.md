# 资源数据处理操作

# 简介
对空间中的资源请求 pfop 持久化数据处理。

### 配置文件选项
```
process=pfop
fops=
pipeline=
```
`process=pfop` 表示请求 pfop 操作  
`fops=` pfop 请求的命令，如 "avthumb/mp4|saveas/dGVzdDoxLm1wMw=="
`pipeline` 进行持久化数据处理的队列名称  
 [pfop 官方文档](https://developer.qiniu.com/dora/manual/3686/pfop-directions-for-use)  

### 命令行方式
```
-process=pfop -fops= -pipeline=
```
