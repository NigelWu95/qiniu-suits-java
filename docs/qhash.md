# 资源查询 qhash

## 简介
对空间中的资源查询 qhash。参考：[七牛资源 hash 值查询](https://developer.qiniu.com/dora/manual/1297/file-hash-value-qhash)  

## 配置文件选项
**操作需指定数据源，请先[配置数据源](../docs/datasource.md)**  

### 配置参数
```
process=qhash 
algorithm=  
domain=
protocol=
url-index=0
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=qhash| 查询qhash时设置为qhash| 表示查询资源 hash 值操作|  
|algorithm| md5/sha1| 查询 qhash 使用的算法,默认为 md5|  
|domain| 域名字符串| 用于拼接文件名生成链接的域名，数据源为 file 且指定 url-index 时无需设置|  
|protocol| http/https| 使用 http 还是 https 访问资源进行抓取（默认 http）|  
|url-index| 字符串| 通过 url 操作时需要设置的 url 索引（下标），需要手动指定才会进行解析|  

### 关于 url-index
当 parse=tab/csv 时 [xx-]index(ex) 设置的下标必须为整数。url-index 表示输入行中存在 url 形式的源文件地址，未设置的情况下则默认从 key 字段
加上 domain 的方式访问源文件地址，key 下标用 indexes 参数设置。  

## 命令行方式
```
-process=qhash -algorithm=md5 -domain= -protocol= 
```
