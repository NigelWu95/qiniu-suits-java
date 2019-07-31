# 云存储数据迁移

## 简介
对不同数据源向七牛存储空间进行数据迁移。

### 迁移说明
数据迁移分为两个基本部分：数据源读取和数据写入，其中数据源读取采用本工具提供的数据源模式（参考：[数据源](datasource.md)），而数据写入（这里表示存
储到七牛空间）采用本公举提供的 asyncfetch 操作（即封装七牛异步抓取接口的 process，参考：[asyncfetch 配置](asyncfetch.md)）。
因此数据迁移操作的配置参数包含两部分，即**数据源配置**和**asyncfetch配置**，配置参考如下：  

#### 1. 阿里云 oss
```
path=aliyun://<bucket>
ali-id=
ali-secret=
#region 可省略，即采用自动判断
region/ali-region=
# private 参数用来指定该数据源空间为私有，需要进行访问签名
private=aliyun

# 迁移配置（七牛的账号、空间等参数）
process=asyncfetch
ak=
sk=
to-bucket=
```

#### 2. 腾讯云 cos
```
path=tencent://<bucket>
ten-id=
ten-secret=
#region 可省略，即采用自动判断
region/ten-region=
# private 参数用来指定该数据源空间为私有，需要进行访问签名
private=tencent

# 迁移配置（七牛的账号、空间等参数）
process=asyncfetch
ak=
sk=
to-bucket=
```

#### 3. Aws S3
```
path=s3://<bucket>
s3-id=
s3-secret=
#region 可省略，即采用自动判断
region/s3-region=
# private 参数用来指定该数据源空间为私有，需要进行访问签名
private=s3

# 迁移配置（七牛的账号、空间等参数）
process=asyncfetch
ak=
sk=
to-bucket=
```

#### 4. 又拍云存储
```
path=upyun://<bucket>
up-name=
up-pass=

# 迁移配置（七牛的账号、空间等参数）
process=asyncfetch
ak=
sk=
to-bucket=
# 又拍云没有直接对资源名进行私有签名的操作，必须提供公开访问域名才能进行迁移
domain=
```

#### 5. 本地文件列表
```
path=<localpath>
parse=
separator=

# 迁移配置（七牛的账号、空间等参数）
process=asyncfetch
ak=
sk=
to-bucket=
# 本地的文件列表为文件名时需要提供能公开访问的 domain，为 url 时需要提供每一行中 url 的坐标名
domain=
url-index=
```  
本地的文件列表作为数据源时需要考虑解析方式和字段下标，参考：[文本文件数据源](datasource.md#2-file-文本文件行读取)  

### filter
当需要对数据源的文件进行筛选之后再迁移，则需要配置 filter 参数，参考：[filter](filter.md)  

### asyncfetch 其他配置
asyncfetch 操作支持回调等参数，具体使用参考：[asyncfetch 配置](asyncfetch.md)  

### 保存任务结果
资源列举结果（云存储文件列表）和异步抓取提交的记录可以保存在自定义的本地目录中，其中资源列举结果可以按照自定义格式来保存，参考：[持久化配置](resultsave.md)  

### 迁移结果查询
由于采用的是异步抓取方式，执行任务后只能保存提交任务的状态，数据迁移状态有两种方式可以知晓：  
1、设置了抓取结果的回调，七牛会对抓取任务的结果进行回调到设置的服务器，如果回调地址异常则无效。  
2、通过再查询的方式才能获取，可根据七牛存储的 stat 接口来查询每个文件的状态从而确认具体文件的迁移结果，本工具提供对应的操作方式，可参考：[stat 配置](stat.md)，
如果迁移过程中保存了列举资源的文件列表，则 stat 操作的数据源可直接配置成文件列表所在目录，不需要再次从原数据空间列举（列举操作比较耗时，建议每次从数
据源列举资源列表时都保存到本地）  