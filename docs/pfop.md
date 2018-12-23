# 资源数据处理操作

# 简介
对空间中的资源请求 pfop 持久化数据处理。

### 配置文件选项
```
process=pfop
pipeline=
force-public=
```
`process=pfop` 表示请求 pfop 操作  
`pipeline` 进行持久化数据处理的队列名称  
`force-public` 是否强制使用共有队列（会有性能影响）  
 [pfop 官方文档](https://developer.qiniu.com/dora/manual/3686/pfop-directions-for-use)  

### 命令行方式
```
-process=pfop -pipeline= -force-public=
```
