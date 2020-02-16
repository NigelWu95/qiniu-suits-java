# 通过 url 同步上传

## 简介
将 url 的内容同步上传至存储空间，同时依据原 url 设置目标文件名。上传可参考：[上传策略](https://developer.qiniu.com/kodo/manual/1206/put-policy) 
和 [流式上传资源](https://developer.qiniu.com/kodo/sdk/1239/java#upload-stream)  
1. **操作需要指定数据源，上传 url 的内容到七牛空间，故需要配置数据源，参考：[配置数据源](datasource.md)**  
2. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置)  
2. 单次上传一个文件请参考[ single 操作](single.md)  
3. 交互式操作随时输入 url 进行上传请参考[ interactive 操作](interactive.md)  

## 配置
```
process=syncupload
ak/qiniu-ak=
sk/qiniu-sk=
region/qiniu-region=
protocol=
domain=
indexes=
url-index=
host=
add-prefix=
rm-prefix=
to-bucket=
expires=
policy.[]=
params.[]=
check=
private=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process|同步 url 内容至存储空间时设置为 syncupload | 表示同步上传 url 内容资源的操作|  
|ak、sk|长度 40 的字符串|抓取到七牛账号的ak、sk，通过七牛控制台个人中心获取|  
|qiniu-ak、qiniu-sk|长度 40 的字符串|抓取到七牛账号的 ak、sk，如果数据源为 qiniu 且目标账号和数据源为同一账号，则无需再设置，如果是跨七牛账号抓取，目标账号的密钥请用 qiniu-ak/qiniu-sk 来设置|  
|region/qiniu-region|存储区域字符串|七牛目标空间的区域，不填时则自动判断，如果选择填写且数据源为七牛另一区域 bucket 时，则目标空间的区域使用 qiniu-region 设置|  
|protocol| http/https| 使用 http 还是 https 访问资源进行下载（默认 http）|  
|domain| 域名字符串| 当数据源数据的资源为文件名列表时，需要设置进行访问的域名，当指定 url-index 时无需设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|url-index| 字符串| 通过 url 操作时需要设置的 [url 索引（下标）](#关于-url-index)，未设置任何索引时根据 parse 类型默认为 0 或 "url"，支持[需要私有签名的情况](#资源需要私有签名)|  
|host| 域名字符串| 下载源资源时指定 host|  
|add-prefix| 字符串| 表示为保存的文件名添加指定前缀|  
|rm-prefix| 字符串| 表示将得到的目标文件名去除存在的指定前缀后再作为保存的文件名|  
|to-bucket| 字符串| 上传资源的目标空间名称|  
|expires| 整型数字| 单个文件上传操作的鉴权有效期，单位 s(秒)，默认为 3600|  
|policy.[]| 字符串/整型数字| 可以设置一些上传策略参数，如 policy.deleteAfterDays=7 表示七天之后自动删除文件，其他参数可参考[七牛上传策略](https://developer.qiniu.com/kodo/manual/1206/put-policy)|  
|params.[]| 字符串| 上传时设置的一些变量参数，如 params.x:user=138300 表示 x:user 的信息为 138300，可参考[七牛上传自定义变量](https://developer.qiniu.com/kodo/manual/1235/vars#xvar)|  
|check|字符串| 进行文件存在性检查，目前可设置为 `stat`，表示通过 stat 接口检查目标文件名是否存在，如果存在则不进行 fetch，而记录为 `file exsits`|  
|private| 数据源私有类型|是否是对私有空间资源进行同步访问上传，选择对应的私有类型，参考[私有访问](#资源需要私有签名)|  

### 超时时间
timeout 参数可以通过全局的 timeout 来设置，参考：[网络设置](../README.md#7-网络设置)  

### 关于 url-index
当使用 file 源且 parse=tab/csv 时 [xx-]index(ex) 设置的下标必须为整数。url-index 表示输入行含 url 形式的源文件地址，未设置的情况下则使用 
key 字段加上 domain 的方式访问源文件地址，key 下标用 indexes 参数设置，参见[ indexes 索引](datasource.md#关于-indexes-索引)。  

### 资源需要私有签名
当进行图片审核的 url 需要通过私有鉴权访问时（资源来自于存储私有权限的空间），本工具支持串联操作，即先进行对应的私有签名再提交审核，使用如下的 private
参数设置即可，如不需要进行私有访问则不设置，目前支持以下几类签名：  
`private=qiniu` [七牛云私有签名](privateurl.md#七牛配置参数)  
`private=tencent` [腾讯云私有签名](privateurl.md#其他存储配置参数)  
`private=aliyun` [阿里云私有签名](privateurl.md#其他存储配置参数)  
`private=s3` [AWS S3 私有签名](privateurl.md#其他存储配置参数)  
`private=huawei` [华为云私有签名](privateurl.md#其他存储配置参数)  
`private=baidu` [百度云私有签名](privateurl.md#其他存储配置参数)  

### 命令行参数方式
```
-process=syncupload -ak= -sk= -to-bucket= -url-index= ...
```

