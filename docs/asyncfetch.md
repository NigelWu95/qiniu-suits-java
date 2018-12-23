# 资源异步抓取操作

# 简介
对文件列表进行异步抓取保存到目标空间。

### 配置文件选项

#### 必须参数
|数据源方式|参数及赋值|
|--------|-----|
|source-type=list（空间资源列举）|process=asyncfetch <br> to-bucket=\<bucket\>|
|source-type=file（文件资源列表）|process=asyncfetch <br> process-ak=\<ak\> <br> process-sk=\<sk\> <br> to-bucket=\<bucket\>|

#### 可选参数
```
keep-key=true
add-prefix=video/
file-type=
ignore-same-key=
domain=
https=
private=false
hash-check=
host=
callback-url=
callback-body=
callback-body-type=
callback-host=
```

### 参数字段说明
|参数名|数据类型 | 含义|  
|-----|-------|-----|  
|process| 异步抓取时设置为 asyncfetch | 表示异步 fetch 操作|  
|process-ak、process-sk|长度 40 的字符串|目标账号的ak、sk，通过七牛控制台个人中心获取，当数据源方式为 list 时无需设置|  
|to-bucket|字符串| 保存抓取结果的空间名|  
|keep-key| true/false| 表示是否使用原文件名来保存抓取的资源，默认为 true|  
|add-prefix| 字符串| 表示为保存的文件名添加指定前缀|  
|ignore-same-key| true/false| 暂未启用|  
|domain| 域名字符串| 当数据源数据的资源为文件名列表时，需要设置进行访问的域名，当数据源方式为 list 且输入文件为 url 列表时无需设置|  
|https| true/false| 是否使用 https 抓取资源（默认否）|  
|private| true/false| 资源域名是否是七牛私有空间的域名（默认否）|  
|hash-check| true/false| 抓取结果是否进行 hash 值校验（默认否）|  
|host| host 字符串| 抓取源资源时指定 host|  
|callback-url| 公网可访问的 url 字符串| 设置回调地址|  
|callback-body| body 字符串| 设置回调 body|  
|callback-body-type| body-type 字符串| 设置回调 body 类型|  
|callback-host| host 字符串| 设置回调 host |   

参考：[七牛异步第三方资源抓取](https://developer.qiniu.com/kodo/api/4097/asynch-fetch)  

### 命令行方式
```
-process=asyncfetch -process-batch=true -process-ak= -process-sk= -to-bucket=bucket2 -keep-key=true -add-prefix=video/ -file-type= -ignore-same-key= -domain= -https= -private= -hash-check= -host= -callback-url= -callback-body= -callback-body-type= -callback-host=
```

