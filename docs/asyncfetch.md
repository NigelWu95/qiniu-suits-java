# 资源异步抓取操作

# 简介
对文件列表进行异步抓取保存到目标空间。

### 配置文件选项
```
process=asyncfetch
process-ak=
process-sk=
to-bucket=bucket
keep-key=true
add-prefix=video/
file-type=
ignore-same-key=
domain=
use-https=
need-sign=true
hash-check=
host=
callback-url=
callback-body=
callback-body-type=
callback-host=
```
`process` 表示异步 fetch 操作  
`process-ak` 目标账号的 ak，未设置时使用 ak 参数的值  
`process-sk` 目标账号的 sk，未设置时使用 sk 参数的值  
`to-bucket` 目标空间  
`keep-key` 表示是否维持原文件名  
`add-prefix` 表示 fetch 之后的文件名添加指定前缀  
`ignore-same-key` 表示是否忽略目标空间中的同名文件  
`domain` 表示用于 fetch 资源的域名  
`use-https` 是否使用 https 访问  
`need-sign` 源链接是否需要七牛私有签名  
`hash-check`  是否进行 hash 值校验  
`host` 访问源资源时指定 host  
`callback-url` 设置回调地址  
`callback-body` 设置回调 body  
`callback-body-type` 设置回调 body 类型  
`callback-host` 设置回调 host   

### 命令行方式
```
-process= -process-batch=true -process-ak= -process-sk= -to-bucket=bucket2 -keep-key=true -add-prefix=video/ -file-type= -ignore-same-key= -domain= -use-https= -need-sign=true -hash-check= -host= -callback-url= -callback-body= -callback-body-type= -callback-host=
```

