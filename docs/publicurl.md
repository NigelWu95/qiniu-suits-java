# 资源 url

## 简介
对资源名拼接域名导出 url。  
1. **操作需要指定数据源，默认表示从七牛空间列举文件执行操作，如非默认或需更多条件，请先[配置数据源](datasource.md)**  
2. 单次对一个资源名 key 生成 url 请参考[ single 操作](single.md)  
3. 交互式操作随时输入资源名 key 生成 url请参考[ interactive 操作](interactive.md)  

## 配置参数
```
process=publicurl
ak=
sk=
protocol=
domain=
indexes=
expires=
queries=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process| 资源 url 操作时设置为 publicurl | 表示为资源生成 url 链接操作|  
|protocol| http/https| 使用 http 还是 https 访问资源进行抓取（默认 http）|  
|domain| 域名字符串| 用于拼接文件名生成链接的域名（七牛存储空间域名可以使用[ domainsfrom 命令查询](domainsofbucket.md)），当指定 url-index 时无需设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|queries| 字符串| url 的 query 参数或样式后缀，如 `?v=1.1&time=1565171107845` 或 `-w480`，[关于 queries 参数](#关于-queries-参数)|  

#### 关于 key
key 下标用 indexes 参数设置，默认会根据 parse 类型设置为 0 或 "key"，参见[ indexes 索引](datasource.md#关于-indexes-索引)。  

#### 关于 queries 参数
queries 参数用于设置 ?+参数部分（或 url 的后缀），希望在 url 上加上参数。  

#### 命令行方式
```
-process=privateurl -ak= -sk= -bucket= 
```
