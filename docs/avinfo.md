# 资源（音视频文件）查询元信息操作

## 简介
对空间中的音视频资源查询 avinfo 元信息。参考：[七牛音视频元信息查询](https://developer.qiniu.com/dora/manual/1247/audio-and-video-metadata-information-avinfo)  

## 配置文件选项
**操作需指定数据源，请先[配置数据源](../docs/datasource.md)**  

### 配置参数
```
process=avinfo 
domain=
protocol=
url-index=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=avinfo| 查询avinfo时设置为avinfo| 表示查询 avinfo 操作|  
|domain| 域名字符串| 用于拼接文件名生成链接的域名，数据源为 file 且指定 url-index 时无需设置|  
|protocol| http/https| 使用 http 还是 https 访问资源进行抓取（默认 http）|  
|url-index| 字符串| 通过 url 操作时需要设置的 url 索引（下标），需要手动指定才会进行解析|  

### 关于 url-index
当使用 file 源且 parse=tab/csv 时下标必须为整数。url-index 表示输入行含 url 形式的源文件地址，未设置的情况下则使用 key 字段加上 domain 的
方式访问源文件地址，key 下标用 indexes 参数设置。  

## 命令行方式
```
-process=avinfo -domain= -protocol=
```
