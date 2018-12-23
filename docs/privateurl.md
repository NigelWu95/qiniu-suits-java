# 私有空间资源签名操作

# 简介
对七牛私有空间中的资源进行签名。

### 配置文件选项
```
process=privateurl
domain=
https=
```
`process=privateurl` 表示私有签名操作  
`domain` 当输入为文件名列表，需要设置域名用于生成资源访问链接，当输入为 url 列表时无需设置  
`https` 表示链接是否使用 https（默认否），当输入为 url 列表时无需设置  

### 命令行方式
```
-process=avinfo -domain= -https= 
```
