# 资源查询 qhash 操作

# 简介
对空间中的资源查询 qhash。

### 配置文件选项
```
process=qhash
domain=
algorithm=md5
use-https=
need-sign=true
```
`process=qhash` 表示查询 qhash 操作  
`domain` 表示用于查询 qhash 的域名  
`algorithm` 查询 qhash 使用的算法，md5/sha1（默认 md5）  
`use-https` 是否使用 https 访问（默认否）  
`need-sign` 源链接是否需要七牛私有签名（默认否）  

### 命令行方式
```
-process=qhash -domain= -algorithm=md5 -use-https= -need-sign= 
```
