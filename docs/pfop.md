# 资源修改、删除操作

# 简介
对文件列表进行修改生命周期、修改存储类型、修改状态、删除操作。

### 配置文件选项
```
process=pfop
fops=
```
`process=pfop` 表示请求 pfop 操作  
`fops=` pfop 请求的命令，如 "avthumb/mp4|saveas/dGVzdDoxLm1wMw=="
 [pfop 官方文档](https://developer.qiniu.com/dora/manual/3686/pfop-directions-for-use)  

### 命令行方式
```
-process=pfop -fops=
```
