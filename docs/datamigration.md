# 数据备份/迁移

## 简介
对不同数据源向七牛存储空间进行数据同步。  
1. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置数据源的密钥，参考：[账号设置](../README.md#账号设置)  

## 迁移说明
数据迁移分为两个基本部分：数据源读取和数据写入，其中数据源读取采用本工具提供的数据源模式（参考：[数据源](datasource.md)），而数据写入（这里表示存
储到七牛空间）可以采用多种方式：  
1、**异步 fetch 操作：`process=asyncfetch`**，根据 url 进行异步的抓取操作，工具只提交任务到七牛服务端，完全由七牛服务端进行任务执行，下载 url
并保存为对象存储文件，适用于大文件迁移场景，且因为是异步处理，可以设置回调等参数，但无法对已提交的任务进行停止，工具只支持按[时间计划](../README.md#12-时间计划)
来提交任务。asyncfetch 具体操作说明和详细配置见[ asyncfetch 配置](asyncfetch.md)。  
2、**同步 fetch 操作：`process=fetch`**，根据 url 进行同步的实时抓取操作，由七牛服务端对 url 进行下载并保存为对象存储文件，工具需要等待七牛返
回抓取结果，响应 200 的即为抓取成功，不适用于大文件，优点是可以随时停止对源站的下载任务，等下一阶段需要时再启动，或者设置[时间计划](../README.md#12-时间计划)。
fetch 具体操作说明和详细配置见[ fetch 配置](fetch.md)。  
3、**镜像源 mirror 操作：`process=mirror`**，对设置了镜像源的空间执行镜像同步操作（对要迁移的源站可以先使用其 IP 或者域名设置为目标空间的镜像
源），根据文件名即可直接从镜像源地址进行资源的同步（以同样的文件名保存到目标空间），优点与 fetch 操作相同。mirror 具体操作说明和详细配置见[ mirror 配置](mirror.md)。  
4、**url 同步上传 syncupload 操作：`process=syncupload`**，根据 url 进行上传操作，通过 url 请求到内容直接上传到目标空间，并根据原 url 的
path 来设置保存的文件名，主要可用于内网下载再上传的场景，达到节省流量的效果。syncupload 具体操作说明和详细配置见[ syncupload 配置](syncupload.md)。  
5、**文件上传 qupload 操作：`process=qupload`**，读取本地目录下的文件进行上传操作，主要针对本地数据源，支持大规模目录下文件的上传，该方式也可以
实现对 NAS 等数据的上传，只需将其挂载到本地即可。qupload 具体操作说明和详细配置见[ qupload 配置](uploadfile.md)。  
6、特殊的，如果是七牛的同区域存储空间之间做备份/迁移，请使用【[copy](copy.md)/[move](move.md) 操作】），如果是跨账号，请先进行[空间授权](https://developer.qiniu.com/kodo/manual/3647/authorization-of-the-space)。

### 过滤和检查
1、filter：当需要对数据源的文件进行过滤之后再迁移，则需要配置 filter 参数，参考：[filter 配置](filter.md)  
2、check：检查目标文件名是否存在，通过工具内置的 stat 操作来检查，设置 `check=stat` 即可  
3、对于异步抓取 asyncfetch 操作，也可以设置 `ignore-same-key=true` 表示存在同名文件时不覆盖原文件  
4、对于上传 qupload/syncupload 操作，也可以设置 `policy.insertOnly=1` 表示使用不覆盖的上传策略，文件名已存在时会直接返回原来的文件信息或者
上传失败，但是依然会消耗上行流量。   

### 保存任务结果
资源列举结果（云存储文件列表）和异步抓取提交的记录可以保存在自定义的本地目录中，其中资源列举结果可以按照自定义格式来保存，参考：[持久化配置](resultsave.md)  

### 迁移结果校验
数据迁移结果需要进行校验完整性，在迁移过程中，建议设置 save-total=true（或者不设置默认为 true），此时会保留从数据源得到的完整列表数据，可统计文件
数量和核对文件名，如果文件数量与迁移到空间中的文件数量一致，说明都迁移成功，如果迁移到空间中的文件数量小于该次从数据源导出的文件数量，则需要进行进一步
校验，确认哪些文件迁移失败，校验有三种方式：  
1、对于异步抓取方式设置了抓取结果的回调，七牛会将抓取任务的结果回调到设置的服务器（callback-url），如果回调地址异常则会回调失败。【集中的迁移任务对
回调服务器的要求较高】  
2、根据数据源的文件名查询七牛存储文件状态，即用七牛存储的 stat 接口来查询每个文件的存储信息从而确认具体文件的迁移结果，本工具提供对应的操作方式，可
参考：[stat 配置](stat.md)，如果迁移过程中保存了列举资源的文件列表，则 stat 操作的数据源可直接配置成文件列表所在目录，不需要再次从原数据空间列举
（列举操作比较耗时，建议每次从数据源列举资源列表时都保存到本地）【stat 查询方式对于大量文件迁移任务校验时间相对较长】  
3、一般对于迁移完的任务都存在下载需求（如果迁移到目标空间是作为低频存储，慎用下载操作，会产生低频读取费用），因此可以在任务结束一段时间后通过 http
下载的方式来校验文件是否迁移成功，而且下载方式一般为 CDN 下载，因此这种方式同时还可以达到 CDN 预热的效果，且可以选择纯预热型下载方式，不保存文件到磁
盘，操作方式参考[ download 文件下载配置](downloadfile.md)。  

因此数据备份/迁移操作的配置参数主要包含两部分，即**数据源配置**和 **process 配置**，配置参考如下：  

#### 1. 阿里云 oss
```
path=aliyun://<bucket>
ali-id=
ali-secret=
#region 可省略，即采用自动判断
region=
# 当数据源空间为私有 bucket 时，private 参数用来指定需要进行访问签名的类型，如果空间为公开空间则不需要该参数
private=aliyun

# 迁移配置（七牛的账号、空间等参数）
process=asyncfetch/fetch/mirror/syncupload
ak=
sk=
to-bucket=
# region 可以不设置则自动判断，如选择自行指定则使用 qiniu-region，便于和数据源的 region 区分开
# qiniu-region=z0/z1/qvm-z0/...
```

#### 2. 腾讯云 cos
```
path=tencent://<bucket>
ten-id=
ten-secret=
#region 可省略，即采用自动判断
region=
# 当数据源空间为私有 bucket 时，private 参数用来指定需要进行访问签名的类型，如果空间为公开空间则不需要该参数
private=tencent

# 迁移配置（七牛的账号、空间等参数）
process=asyncfetch/fetch/mirror/syncupload
ak=
sk=
to-bucket=
# region 可以不设置则自动判断，如选择自行指定则使用 qiniu-region，便于和数据源的 region 区分开
# qiniu-region=z0/z1/qvm-z0/...
```

#### 3. Aws S3
```
path=s3://<bucket>
s3-id=
s3-secret=
#region 可省略，即采用自动判断
region=
# 当数据源空间为私有 bucket 时，private 参数用来指定需要进行访问签名的类型，如果空间为公开空间则不需要该参数
private=s3

# 迁移配置（七牛的账号、空间等参数）
process=asyncfetch/fetch/mirror/syncupload
ak=
sk=
to-bucket=
# region 可以不设置则自动判断，如选择自行指定则使用 qiniu-region，便于和数据源的 region 区分开
# qiniu-region=z0/z1/qvm-z0/...
```

#### 4. 又拍云存储
```
path=upyun://<bucket>
up-id=
up-secret=

# 迁移配置（七牛的账号、空间等参数）
process=asyncfetch/fetch/mirror/syncupload
ak=
sk=
to-bucket=
# region 可以不设置则自动判断，如选择自行指定则使用 qiniu-region，便于和数据源的 region 区分开（不过又拍云存储目前还没有区域概念，可以直接使用 region 设置七牛目标 bucket 的区域）
# qiniu-region=z0/z1/qvm-z0/...

# 又拍云没有直接对资源名进行私有签名的操作，必须提供公开访问域名才能进行迁移
domain=
```

#### 5. 华为云 obs
```
path=huawei://<bucket>
hua-id=
hua-secret=
#region 可省略，即采用自动判断
region/hua-region=
# 当数据源空间为私有 bucket 时，private 参数用来指定需要进行访问签名的类型，如果空间为公开空间则不需要该参数
private=huawei

# 迁移配置（七牛的账号、空间等参数）
process=asyncfetch/fetch/mirror/syncupload
ak=
sk=
to-bucket=
# region 可以不设置则自动判断，如选择自行指定则使用 qiniu-region，便于和数据源的 region 区分开
# qiniu-region=z0/z1/qvm-z0/...
```

#### 6. 百度云 bos
```
path=baidu://<bucket>
bai-id=
bai-secret=
#region 可省略，即采用自动判断
region/bai-region=
# 当数据源空间为私有 bucket 时，private 参数用来指定需要进行访问签名的类型，如果空间为公开空间则不需要该参数
private=baidu

# 迁移配置（七牛的账号、空间等参数）
process=asyncfetch/fetch/mirror/syncupload
ak=
sk=
to-bucket=
# region 可以不设置则自动判断，如选择自行指定则使用 qiniu-region，便于和数据源的 region 区分开
# qiniu-region=z0/z1/qvm-z0/...
```

#### 7. 七牛云 qos
```
path=qiniu://<bucket>
ak=
sk=
# region 可省略，即采用自动判断
region=
# 当数据源空间为私有 bucket 时，private 参数用来指定需要进行访问签名的类型，如果空间为公开空间则不需要该参数
private=qiniu

# 迁移配置（七牛的目标账号、空间等参数）
process=asyncfetch/fetch/mirror/syncupload
# 由于数据源中可能设置了另外的 ak/sk，这里抓取资源到另一个账号的话，便于区分开则使用 qiniu-ak/qiniu-sk 作为目标账号，如抓取同一账号下的文件，则
# 不用再设置 qiniu-ak/qiniu-sk。如果是同一区域不同账号或空间的资源，强烈建议使用空间授权之后做 move/copy，节省流量且提高效率
qiniu-ak=
qiniu-sk=
to-bucket=
# region 可以不设置则自动判断，如选择自行指定则使用 qiniu-region，便于和数据源的 region 区分开
# qiniu-region=z0/z1/qvm-z0/...
```  

##### 七牛云私有存储
按需要增加如下参数设置 API 的域名即可：  
```
rsf-domain=rsf-z0.qiniu.com
#rsf-domain=rsf-z1.qiniu.com
rs-domain=rs-z0.qiniu.com
#rs-domain=rs-z1.qiniu.com
api-domain=api.qiniu.com
#api-domain=api-z1.qiniu.com
#是否使用 https 对上述设置的域名进行请求
config-https=true/false
```  

#### 8. 本地文件列表
```
path=<localpath>
parse=
separator=
# 当数据源 url 为私有空间的资源裸链接时，private 参数用来指定该需要进行访问签名的类型，如果空间为公开空间则不需要该参数
private=aliyun/qiniu/tencent/s3/huawei/baidu

# 迁移配置（七牛的账号、空间等参数）
process=asyncfetch/fetch/mirror/syncupload
ak=
sk=
to-bucket=
# region 可以不设置则自动判断，如选择自行指定则使用 qiniu-region，便于和数据源的 region 区分开（不过 local file 数据源是没有区域设置的，除非使用了带 region 的数据源的私有签名 private 设置）
# qiniu-region=z0/z1/qvm-z0/...

# 本地的文件列表为文件名时需要提供能公开访问的 domain，为 url 时需要提供每一行中 url 的坐标名
domain=
url-index=
```  
（1）如是对文件执行上传，请参考[ qupload 配置](uploadfile.md)。  
（2）本地的文件列表作为数据源时需要考虑解析方式和字段下标，参考：[本地文件数据源](datasource.md#2-file-本地文件读取)  