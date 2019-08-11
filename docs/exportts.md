# 导出 ts 列表操作

## 简介
对 m3u8 的资源链接进行读取导出其中的 ts 文件列表。  

## 配置文件
**操作需指定数据源，请先[配置数据源](datasource.md)**  

### 配置参数
```
process=exportts
protocol=
domain=
indexes=
url-index=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=exportts| 从 m3u8 导出 ts 时设置为exportts| 表示导出 ts 操作|  
|protocol| http/https| 使用 http 还是 https 访问资源进行抓取（默认 http）|  
|domain| 域名字符串| 用于拼接文件名生成链接的域名，当指定 url-index 时无需设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|url-index| 字符串| 通过 url 操作时需要设置的 url 索引（下标），需要手动指定才会进行解析|  

### 关于 url-index
当使用 file 源且 parse=tab/csv 时 [xx-]index(ex) 设置的下标必须为整数。url-index 表示输入行含 url 形式的源文件地址，未设置的情况下则使用 
key 字段加上 domain 的方式访问源文件地址，key 下标用 indexes 参数设置。  

## 命令行方式
```
-process=avinfo -protocol= -domain=
```
