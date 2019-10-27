# 文件下载/预下载

## 简介
下载文件列表的资源到本地。   
1. **操作需要指定数据源，默认表示从七牛空间列举文件执行操作，如非默认或需更多条件，请先[配置数据源](datasource.md)**  

## 配置文件

### 功能配置参数
```
process=download
protocol=
domain=
indexes=
url-index=
host=
bytes=
queries=
pre-down=
add-prefix=
rm-prefix=
download-timeout=
save-path=
private=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process| 下载资源时设置为download | 表示资源下载操作|  
|protocol| http/https| 使用 http 还是 https 访问资源进行下载（默认 http）|  
|domain| 域名字符串| 当数据源数据的资源为文件名列表时，需要设置进行访问的域名，当指定 url-index 时无需设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|url-index| 字符串| 通过 url 操作时需要设置的 [url 索引（下标）](#关于-url-index)，需要手动指定才会进行解析，支持[需要私有签名的情况](#url-需要私有签名访问)|  
|host| 域名字符串| 下载源资源时指定 host|  
|bytes| 字节范围| 下载源资源时指定 range 的范围，如 `0-1024`，只有一个数字时表示起始字节，如 `1024-` 表示从 1024 到结尾，可用于大文件预热起始字节的场景|  
|queries| 字符串| url 的 query 参数或样式后缀，如 `-w480` 或 `?v=1.1&time=1565171107845`（这种形式请务必带上 ? 号，否则无效）[关于 queries 参数](#关于-queries-参数)|  
|pre-down| true/false|为 true 时表示预下载，即下载的内容不保存为文件，为 false 表示保存成本地文件，默认为 false|  
|add-prefix| 字符串| 表示为保存的文件名添加指定前缀|  
|rm-prefix| 字符串| 表示将得到的目标文件名去除存在的指定前缀后再作为保存的文件名|  
|download-timeout| 时间，单位秒|设置下载文件的超时时间，默认 1200s，下载大文件可根据需要调整|  
|save-path| 文件保存路径|设置下载文件的保存路径，为本地的目录名称|  
|private| 数据源私有类型|是否是对私有空间资源进行下载，选择对应的私有类型，参考[私有访问](#资源需要私有签名)|  

### 关于 queries 参数
queries 参数用于设置 url 的后缀或 ?+参数部分，资源下载可能需要下载不同格式或尺寸的如图片文件，因此可以通过一些图片处理样式或参数来设置对处理之后的
图片进行下载。当设置 private（私有签名）的情况下，该参数会使用在 privateurl 操作中（因为 privateurl 操作在前，当前操作在后）。  

### 关于 url-index
当使用 file 源且 parse=tab/csv 时 [xx-]index(ex) 设置的下标必须为整数。url-index 表示输入行含 url 形式的源文件地址，未设置的情况下则使用 
key 字段加上 domain 的方式访问源文件地址，key 下标用 indexes 参数设置。  

### 资源需要私有签名
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
-process=download -ak= -sk= -protocol= -domain= -host= -add-prefix= -save-path= ...
```

