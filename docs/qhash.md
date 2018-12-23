# 资源查询 qhash

# 简介
对空间中的资源查询 qhash。参考：[七牛资源 hash 值查询](https://developer.qiniu.com/dora/manual/1297/file-hash-value-qhash)  

### 配置文件选项

#### 必须参数
|数据源方式|参数及赋值|
|--------|-----|
|source-type=list（空间资源列举）|[list 数据源参数](listbucket.md) <br> process=qhash <br> domain=\<domain> |  
|source-type=file（文件资源列表）|[file 数据源参数](fileinput.md) <br> process=qhash <br> [domain=\<domain>] |  

#### 可选参数
```
domain=
algorithm=  
https=
private=false
ak=
sk=
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=qhash| 查询qhash时设置为qhash| 表示查询资源 hash 值操作|  
|algorithm| md5/sha1| 查询 qhash 使用的算法,默认为 md5|  
|domain| 域名字符串| 用于拼接文件名生成链接的域名，数据源为 file 且输入列表为 url 时无需设置|  
|https| true/false| 设置 domain 的情况下可以选择是否使用 https 访问（默认否）|  
|private| true/false| 资源域名是否是七牛私有空间的域名（默认否）|  
|ak、sk| | private=true且source-type=file时必填（默认无）|  


### 命令行方式
```
-process=qhash -domain= -algorithm=md5 -https= -private= -ak= -sk= 
```
