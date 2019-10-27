# 内容审核

## 简介
对图片/视频资源进行内容审核。参考：[七牛内容审核](https://developer.qiniu.com/censor)  
1. **操作需指定数据源，请先[配置数据源](datasource.md)**  
2. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置)  

## 配置文件

### 功能配置参数

#### 图片审核
审核 image 类型的资源，同步审核，审核结果输出为 json：[七牛图片审核响应 json](https://developer.qiniu.com/censor/api/5588/image-censor#4)
如果数据源的资源类型不确定（如云存储数据源），建议设置 filter 选项：f-mime=image  
```
process=imagecensor
ak=
sk=
protocol=
domain=
indexes=
url-index=
queries=
scenes=
private=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process| 图片审核时设置为imagecensor | 表示图片资源的内容审核操作|  
|ak、sk|长度 40 的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源为 qiniu 时无需再设置|  
|protocol| http/https| 使用 http 还是 https 访问资源进行抓取（默认 http）|  
|domain| 域名字符串| 当数据源数据为文件名列表时，需要设置进行访问的域名，当指定 url-index 时无需设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|url-index| 字符串| 通过 url 操作时需要设置的 [url 索引（下标）](#关于-url-index)，需要手动指定才会进行解析，支持[需要私有签名的情况](#url-需要私有签名访问)|  
|queries| 字符串| url 的 query 参数或样式后缀，如 `-w480` 或 `?v=1.1&time=1565171107845`（这种形式请务必带上 ? 号，否则无效）[关于 queries 参数](#关于-queries-参数)|  
|scenes| 审核类型字符串| pulp/terror/politician，鉴黄、鉴暴恐、鉴政，多种类型同时审核可用 `,` 拼接，如：`pulp,terror` 或 `pulp,terror,politician` 等|  
|private| 数据源私有类型|是否是对私有空间资源进行审核，选择对应的私有类型，参考[私有访问](#url-需要私有签名访问)|  

##### 关于 queries 参数
queries 参数用于设置 url 的后缀或 ?+参数部分，内容审核可能会出现大图超过尺寸或大小导致失败，因此可以通过一些图片处理样式或参数来设置对处理之后的图
片进行审核。当设置 private（私有签名）的情况下，该参数会使用在 privateurl 操作中（因为 privateurl 操作在前，当前操作在后）。  

#### 视频审核
审核 video 类型的资源，异步审核，审核结果输出为 jobId，要获取进一步的实际审核结果需要通过 id 查询，参考该工具的 [censorresult 操作](censorresult.md)，
七牛官网文档见：[通过jobid获取视频审核结果](https://developer.qiniu.com/censor/api/5620/video-censor#4)，如果数据源的资源类型不确定
（如云存储数据源），建议设置 filter 选项：f-mime=video
```
process=videocensor
ak=
sk=
protocol=
domain=
indexes=
url-index=
scenes=
interval=
saver-bucket=
saver-prefix=
callback-url=
check-url=
private=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process| 视频审核时设置为videocensor | 表示视频资源的内容审核操作|  
|ak、sk|长度 40 的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源为 qiniu 时无需再设置|  
|protocol| http/https| 使用 http 还是 https 访问资源进行抓取（默认 http）|  
|domain| 域名字符串| 当数据源数据为文件名列表时，需要设置进行访问的域名，当指定 url-index 时无需设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|url-index| 字符串| 通过 url 操作时需要设置的 [url 索引（下标）](#关于-url-index)，需要手动指定才会进行解析，支持[需要私有签名的情况](#url-需要私有签名访问)|  
|scenes| 审核类型字符串| pulp/terror/politician，鉴黄、鉴暴恐、鉴政，多种类型同时审核可用 `,` 拼接，如：`pulp,terror` 或 `pulp,terror,politician` 等|  
|interval| 整型，单位 ms| 视频审核需要截帧，此参数设置截帧间隔，默认为5000（5s)|  
|saver-bucket| bucket名称|视频截帧产生的帧图进行保存的空间，不设置则不保存，默认不保存|  
|saver-prefix| 字符串| 视频截帧产生的帧图进行保存的文件名前缀，默认无|  
|callback-url| 公网可访问的 url 字符串| 设置回调地址|  
|check-url| true/false|表示是否在提交任务之前对回调地址进行简单的 post 请求验证（无body的纯post请求），默认为 true，如果无需验证则设置为 false|  
|private| 数据源私有类型|是否是对私有空间资源进行审核，选择对应的私有类型，参考[私有访问](#资源需要私有签名)|  

#### 关于 url-index
当使用 file 源且 parse=tab/csv 时 [xx-]index(ex) 设置的下标必须为整数。url-index 表示输入行含 url 形式的源文件地址，未设置的情况下则使用 
key 字段加上 domain 的方式访问源文件地址，key 下标用 indexes 参数设置。  

#### 资源需要私有签名
当进行图片审核的 url 需要通过私有鉴权访问时（资源来自于存储私有权限的空间），本工具支持串联操作，即先进行对应的私有签名再提交审核，使用如下的 private
参数设置即可，如不需要进行私有访问则不设置，目前支持以下几类签名：  
`private=qiniu` [七牛云私有签名](privateurl.md#七牛配置参数)  
`private=tencent` [腾讯云私有签名](privateurl.md#其他存储配置参数)  
`private=aliyun` [阿里云私有签名](privateurl.md#其他存储配置参数)  
`private=s3` [AWS S3 私有签名](privateurl.md#其他存储配置参数)  
`private=huawei` [华为云私有签名](privateurl.md#其他存储配置参数)  
`private=baidu` [百度云私有签名](privateurl.md#其他存储配置参数)  

## 命令行参数方式
```
-process=imagecensor/videocensor -ak= -sk= -protocol= -domain= ...
```

