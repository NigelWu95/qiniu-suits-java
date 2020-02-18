# 资源异步抓取

## 简介
对文件列表进行异步抓取保存到目标空间。参考：[七牛异步第三方资源抓取](https://developer.qiniu.com/kodo/api/4097/asynch-fetch)  
1. **操作需指定数据源，请先[配置数据源](datasource.md)**  
2. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置)  
3. 单次抓取一个文件请参考[ single 操作](single.md)  
4. 交互式操作随时输入 url 进行抓取请参考[ interactive 操作](interactive.md)  

## 配置
```
process=asyncfetch
ak/qiniu-ak=
sk/qiniu-sk=
region/qiniu-region=
protocol=
domain=
indexes=
url-index=
add-prefix=
rm-prefix=
to-bucket=
check=
host=
callback-url=
callback-body=
callback-body-type=
callback-host=
file-type=
ignore-same-key=
check-url=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process| 异步抓取操作时设置为 asyncfetch | 表示异步 fetch 操作|  
|ak、sk|长度 40 的字符串|抓取到七牛账号的ak、sk，通过七牛控制台个人中心获取|  
|qiniu-ak、qiniu-sk|长度 40 的字符串|抓取到七牛账号的 ak、sk，如果数据源为 qiniu 且目标账号和数据源为同一账号，则无需再设置，如果是跨七牛账号抓取，目标账号的密钥请用 qiniu-ak/qiniu-sk 来设置|  
|region/qiniu-region|存储区域字符串|七牛目标空间的区域，不填时则自动判断，如果选择填写且数据源为七牛另一区域 bucket 时，则目标空间的区域使用 qiniu-region 设置|  
|protocol| http/https| 使用 http 还是 https 访问资源进行抓取（默认 http）|  
|domain| 域名字符串| 用于拼接文件名生成链接的域名（七牛存储空间域名可以使用[ domainsfrom 命令查询](domainsofbucket.md)），当指定 url-index 时无需设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|url-index| 字符串| 通过 url 操作时需要设置的 url 索引（下标），未设置任何索引和 domain 时根据 parse 类型默认为 0 或 "url"|  
|add-prefix| 字符串| 表示为保存的文件名添加指定前缀|  
|rm-prefix| 字符串| 表示将得到的目标文件名去除存在的指定前缀后再作为保存的文件名|   
|to-bucket|字符串| 保存抓取结果的空间名|  
|check|字符串| 进行文件存在性检查，目前可设置为 `stat`，表示通过 stat 接口检查目标文件名是否存在，如果存在则不进行 fetch，而记录为 `file exsits`|  
|host| host 字符串|抓取源资源时指定 host|  
|md5-index| 字符串| 资源 md5 值索引（下标），需要手动指定才会进行解析|  
|callback-url| 公网可访问的 url 字符串| 设置回调地址|  
|callback-body| body 字符串| 设置回调 body|  
|callback-body-type| body-type 字符串| 设置回调 body 类型|  
|callback-host| 域名字符串| 设置回调 host |  
|file-type| 0/1| 文件的存储类型，默认为 0 标准存储|  
|ignore-same-key| true/false|为 false 时表示覆盖同名文件，为 true 表示不覆盖|  
|check-url| true/false|表示是否在提交任务之前对回调地址进行简单的 post 请求验证（无body的纯post请求），默认为 true，如果无需验证则设置为 false|  

### 关于 url-index 和 md5-index
当使用 file 数据源且 parse=tab/csv 时 [xx-]index(ex) 设置的下标必须为整数。url-index 表示输入行中含 url 的字段索引，未设置的情况下则使用
key 字段加上 domain 的方式访问源文件地址，key 下标用 indexes 参数设置，默认会根据 parse 类型设置为 0 或 "key"，参见[ indexes 索引](datasource.md#关于-indexes-索引)，md5-index 为需要进行 md5 校验时输入 md5 值的字段下标，不设置则无效。  

### 命令行参数方式
```
-process=asyncfetch -ak= -sk= -to-bucket= -add-prefix= -protocol= -domain= -host= -callback-url= -callback-body= -callback-body-type= -callback-host= -file-type= -ignore-same-key=
```

