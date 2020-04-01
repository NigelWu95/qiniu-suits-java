# 资源镜像更新

## 简介
对空间中的资源进行镜像源更新。参考：[七牛空间资源镜像更新](https://developer.qiniu.com/kodo/api/1293/prefetch)
1. **操作需要指定数据源，默认表示从七牛空间列举文件执行操作，如非默认或需更多条件，请先[配置数据源](datasource.md)**  
2. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置)  
3. 单次更新一个文件请参考[ single 操作](single.md)  
4. 交互式操作随时输入 key 进行更新请参考[ interactive 操作](interactive.md)  

## 配置
> config.txt
```
path=
process=mirror
ak/qiniu-ak=
sk/qiniu-sk=
region/qiniu-region=
to-bucket=
indexes=
check=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=mirror| 资源镜像更新时设置为mirror| 表示资源镜像更新操作|  
|ak、sk|长度 40 的字符串|抓取到七牛账号的ak、sk，通过七牛控制台个人中心获取|  
|qiniu-ak、qiniu-sk|长度 40 的字符串|抓取到七牛账号的ak、sk，如果数据源为 qiniu 且目标账号和数据源为同一账号，则无需再设置，如果是跨七牛账号抓取，目标账号的密钥请用 qiniu-ak/qiniu-sk 来设置| 
|region/qiniu-region|存储区域字符串|七牛目标空间的区域，不填时则自动判断，如果选择填写且数据源为七牛另一区域 bucket 时，则目标空间的区域使用 qiniu-region 设置|  
|to-bucket|字符串| 设置了镜像源的目标空间名称|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|check|字符串| 进行文件存在性检查，目前可设置为 `stat`，表示通过 stat 接口检查目标文件名是否存在，如果存在则不进行 fetch，而记录为 `file exsits`|  

运行参数：`-config=config.txt`

### 命令行方式
```
-path= -process=mirror -ak= -sk= -to-bucket=  
```
