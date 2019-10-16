# 修改资源 metadata

## 简介
修改存储空间文件的 metadata，修改之后访问文件时会体现在响应头中。可参考：[七牛资源元信息修改](https://developer.qiniu.com/kodo/api/1252/chgm)  
1. **操作需要指定数据源，目前仅支持上传本地文件到七牛空间，故需要配置本地数据源，参考：[配置数据源](datasource.md)**  
2. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置)  

## 配置文件

### 功能配置参数
```
process=metadata
ak=
sk=
bucket=
meta.[]=
cond.[]=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process|上传资源时设置为 mime | 表示修改资源的 mimeType 操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取|  
|bucket| 字符串| 资源所在空间名称|  
|meta.[]| 字符串| metadata 的设置，全局设置，应用在所有文件上进行修改，支持任意自定义字段，也可以是常用的标准字段，如 meta.key1=value1, meta.Cache-Control=public, max-age=36000|  
|cond.[]| 字符串| 可以设置一些修改操作时的条件 condition，cond 当前支持设置 hash、mime、fsize、putTime 条件，只有条件匹配才会执行修改操作，如 cond.mime=text/plain|  

## 命令行参数方式
```
-process=metadata -ak= -sk= -bucket= -meta.Cache-Control=no store ...
```

