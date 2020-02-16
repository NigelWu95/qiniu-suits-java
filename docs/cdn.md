# cdn 相关操作

## 简介
对七牛 CDN 资源进行刷新或者预取，因为该操作敏感，存在 qps 限制，故不支持设置线程数，始终为单线程。支持：  
1. **操作需要指定数据源，默认表示从七牛空间列举文件执行操作，如非默认或需更多条件，请先[配置数据源](datasource.md)**  
2. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置)  
3. 单次对一个 url 刷新/预取请参考[ single 操作](single.md)  
4. 交互式操作随时输入 key/url 进行刷新/预取请参考[ interactive 操作](interactive.md)  

## 配置
### CDN 刷新缓存
```
process=cdnrefresh
ak=
sk=
protocol=
domain=
indexes=
url-index=
is-dir=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process| cdn 缓存刷新时设置为 cdnrefresh | 表示 cdn 缓存刷新操作|  
|ak、sk|长度40的字符串|七牛账号的 ak、sk，通过七牛控制台个人中心获取，当数据源（如 path=<source>://<bucket>）指定时无需再设置|  
|protocol| http/https| 使用 http 还是 https 访问资源进行抓取（默认 http）|  
|domain| 域名字符串| 用于拼接文件名生成链接的域名（七牛存储空间域名可以使用[ domainsfrom 命令查询](domainsofbucket.md)），当指定 url-index 时无需设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|url-index| 字符串| 通过 url 操作时需要设置的 url 索引（下标），未设置任何索引时根据 parse 类型默认为 0 或 "url"|  
|is-dir| true/false| 是否进行目录刷新，设置为 true 时，输入的 url 或 key 必须是目录形式（即以 / 结尾），默认为 false，表示进行 url 刷新|  

#### 关于 url-index
当 parse=tab/csv 时 [xx-]index(ex) 设置的下标必须为整数。url-index 表示输入行中存在 url 形式的源文件地址，未设置的情况下则默认从 key 字段
加上 domain 的方式访问源文件地址，key 下标用 indexes 参数设置，参见[ indexes 索引](datasource.md#关于-indexes-索引)。  

#### 命令行方式
```
-process=cdnrefresh -ak= -sk= -bucket= 
```

### CDN 资源预取
```
process=cdnprefetch
ak=
sk=
protocol=
domain=
indexes=
url-index=
``` 
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process| cdn 缓存刷新时设置为 cdnrefresh | 表示 cdn 缓存刷新操作|  
|ak、sk|长度40的字符串|七牛账号的 ak、sk，通过七牛控制台个人中心获取，当数据源（如 path=<source>://<bucket>）指定时无需再设置|  
|protocol| http/https| 使用 http 还是 https 访问资源进行抓取（默认 http）|  
|domain| 域名字符串| 用于拼接文件名生成链接的域名（七牛存储空间域名可以使用[ domainsfrom 命令查询](domainsofbucket.md)），当指定 url-index 时无需设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|url-index| 字符串| 通过 url 操作时需要设置的 url 索引（下标），未设置任何索引时根据 parse 类型默认为 0 或 "url"|  

#### 命令行方式
```
-process=cdnprefetch -ak= -sk= -bucket= 
```