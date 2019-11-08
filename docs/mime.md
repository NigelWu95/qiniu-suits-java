# 修改资源 mimeType（即 content-type）

## 简介
修改存储空间文件的 mimeType。可参考：[七牛资源元信息修改](https://developer.qiniu.com/kodo/api/1252/chgm)  
1. **操作需要指定数据源，目前仅支持上传本地文件到七牛空间，故需要配置本地数据源，参考：[配置数据源](datasource.md)**  
2. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置)  
3. 单次修改一个文件请参考[ single 操作](single.md)  
4. 交互式操作随时输入 key 进行修改请参考[ interactive 操作](interactive.md)  

## 配置
```
process=mime
ak=
sk=
bucket=
mime=
cond.[]=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process|上传资源时设置为 mime | 表示修改资源的 mimeType 操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取|  
|bucket| 字符串| 资源所在空间名称|  
|mime| 字符串| 非必填字端，如果设置表示所有文件均设置为该 mime 类型，否则需要从每一行中获取 mime 的值，mime 值的索引通过 [indexes](datasource.md#关于-indexes-索引) 的第五个字段获取|  
|cond.[]| 字符串| 可以设置一些修改操作时的条件 condition，cond 当前支持设置 hash、mime、fsize、putTime 条件，只有条件匹配才会执行修改操作，如 cond.mime=text/plain|  

### 命令行参数方式
```
-process=mime -ak= -sk= -bucket= -mime= ...
```

