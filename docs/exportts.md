# 导出 ts 列表操作

## 简介
对 m3u8 的资源链接进行读取导出其中的 ts 文件列表。   
1. **操作需要指定数据源，默认表示从七牛空间列举文件执行操作，如非默认或需更多条件，请先[配置数据源](datasource.md)**  
3. 单次导出一个 m3u8 的 ts 请参考[ single 操作](single.md)  
4. 交互式操作随时输入 url 进行导出请参考[ interactive 操作](interactive.md)  

## 配置
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
|domain| 域名字符串| 用于拼接文件名生成链接的域名（七牛存储空间域名可以使用[ domainsfrom 命令查询](domainsofbucket.md)），当指定 url-index 时无需设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|url-index| 字符串| 通过 url 操作时需要设置的 url 索引（下标），未设置任何索引时根据 parse 类型默认为 0 或 "url"|  

### 关于 url-index
当使用 file 源且 parse=tab/csv 时 [xx-]index(ex) 设置的下标必须为整数。url-index 表示输入行含 url 形式的源文件地址，未设置的情况下则使用 
key 字段加上 domain 的方式访问源文件地址，key 下标用 indexes 参数设置，参见[ indexes 索引](datasource.md#关于-indexes-索引)。  

### 命令行方式
```
-process=avinfo -protocol= -domain=
```
