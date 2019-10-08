# 文件批量上传

## 简介
将本地的文件批量上传至存储空间。  
1. **操作需要指定数据源，目前仅支持上传本地文件到七牛空间，故需要配置本地数据源，参考：[配置数据源](datasource.md)**  
2. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置（7.73-及以上版本）)  

## 配置文件

### 功能配置参数
支持通过 `-a=<account-name>`/`-d` 使用账号设置，参考：[账号设置](../README.md#账号设置（7.73-及以上版本）)  
```
process=qupload
path=
ak=
sk=
bucket=
filepath-index=
parent-path=
record=
keep-path=
add-prefix=
rm-prefix=
expires=
policy.[]=
params.[]=
crc=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process|上传资源时设置为qupload | 表示资源下载操作|  
|path| 本地路径| path 是数据源选项，可以通过设置本地路径来指定要上传的文件，为目录时会遍历目录下（包括内层目录）除隐藏文件外的所有文件|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取|  
|bucket| 字符串| 上传到的资源原空间名称|  
|filepath-index| 文件路径索引| 非必填字端，当直接上传 path 路径中的文件时无需设置，如果是通过读取文本文件每一行中的路径信息则需要设置|  
|parent-path|上级目录| 该参数通常和 filepath-index 同时使用，用于规定文本中的路径值拼接上层目录得到要上传的文件路径|  
|record| true/false| 对于大于 4M 的文件会自动使用分片上传，该参数用于规定分片上传是否记录上传进度信息（断点续传作用），默认不开启|  
|keep-path| true/false| 上传到空间的文件名（资源 key）是否保存从 path 开始的完整路径，默认为 false，则只使用文件最后的名称作为空间的资源 key|  
|add-prefix| 字符串| 表示为保存的文件名添加指定前缀|  
|rm-prefix| 字符串| 表示将得到的目标文件名去除存在的指定前缀后再作为保存的文件名|  
|expires| 整型数字| 单个文件上传操作的鉴权有效期，单位 s(秒)，默认为 3600|  
|policy.[]| 字符串/整型数字| 可以设置一些上传策略参数，如 policy.deleteAfterDays=7 表示七天之后自动删除文件，其他参数可参考[七牛上传策略](https://developer.qiniu.com/kodo/manual/1206/put-policy)|  
|params.[]| 字符串| 上传时设置的一些变量参数，如 params.x:user=138300 表示 x:user 的信息为 138300，可参考[七牛上传自定义变量](https://developer.qiniu.com/kodo/manual/1235/vars#xvar)|  
|crc| true/false| 是否开启 crc32 来校验文件的上传，默认为 false|  
timeout 参数可以通过全局的 timeout 来设置，参考：[超时设置](../README.md#7-超时设置)  

### 关于 filepath-index
当使用 file 源且 parse=tab/csv 时 [xx-]index(ex) 设置的下标必须为整数。filepath-index 表示输入行含 filepath 形式的文件路径，未设置的情
况下则使用 key 字段加上 parent-path 的方式访问文件路径，key 下标用 indexes 参数设置。  

## 命令行参数方式
```
-process=qupload -ak= -sk= -bucket= -path= ...
```

