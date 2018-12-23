# 资源（音视频文件）查询元信息操作

# 简介
对空间中的音视频资源查询 avinfo 元信息。

### 配置文件选项
```
process=avinfo
domain=
https=
private=false
```
`process=avinfo` 表示查询 avinfo 操作  
`domain` 表示用于查询 avinfo 的域名  
`https` 是否使用 https 访问（默认否）  
`private` 资源域名是否为七牛私有空间域名（默认否）  

#### 必须参数
```
process=avinfo
```

#### 可选参数
```
domain=
https=
private=false
```

### 参数字段说明
|参数名|数据类型 | 含义|  
|-----|-------|-----|  
|process| 异步抓取时设置为 asyncfetch | 表示异步 fetch 操作|  
|process-ak、process-sk|长度 40 的字符串|目标账号的ak、sk，通过七牛控制台个人中心获取，当数据源方式为 list 时无需设置|  
|to-bucket|字符串| 保存抓取结果的空间名|  
|keep-key| true/false| 表示是否使用原文件名来保存抓取的资源，默认为 true|  
|add-prefix| 字符串| 表示为保存的文件名添加指定前缀|  

参考：[七牛异步第三方资源抓取](https://developer.qiniu.com/kodo/api/4097/asynch-fetch)  

### 命令行方式
```
-process=avinfo -domain= -https= -private= 
```
