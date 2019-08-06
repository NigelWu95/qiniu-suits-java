# 资源异步抓取

## 简介
下载文件列表的资源到本地。  

## 配置文件
**操作需指定数据源，请先[配置数据源](datasource.md)**  

### 功能配置参数
```
process=download
domain=
protocol=
indexes=
url-index=
host=
add-prefix=
rm-prefix=
indexes=
pre-down=
download-timeout=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process| 下载资源时设置为download | 表示资源下载操作|  
|domain| 域名字符串| 当数据源数据的资源为文件名列表时，需要设置进行访问的域名，当指定 url-index 时无需设置|  
|protocol| http/https| 使用 http 还是 https 访问资源进行下载（默认 http）|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|url-index| 字符串| 通过 url 操作时需要设置的 url 索引（下标），需要手动指定才会进行解析|  
|add-prefix| 字符串| 表示为保存的文件名添加指定前缀|  
|rm-prefix| 字符串| 表示将得到的目标文件名去除存在的指定前缀后再作为保存的文件名|  
|host| 域名字符串| 下载源资源时指定 host|  
|pre-down| true/false|为 true 时表示预下载，即下载的内容不保存为文件，为 false 表示保存成本地文件，默认为 false|  
|download-timeout| 时间，单位秒|设置下载文件的超时时间，默认 1200s，下载大文件可根据需要调整|  

### 关于 url-index
当使用 file 源且 parse=tab/csv 时 [xx-]index(ex) 设置的下标必须为整数。url-index 表示输入行含 url 形式的源文件地址，未设置的情况下则使用 
key 字段加上 domain 的方式访问源文件地址，key 下标用 indexes 参数设置。  

## 命令行参数方式
```
-process=download -ak= -sk= -to-bucket= -add-prefix= -domain= -protocol= -host= ...
```

