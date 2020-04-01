# 内容审核结果查询

## 简介
对（视频）内容审核返回的 jobId 查询 censorresult（审核的结果查询）操作。参考：[七牛内容审核结果查询](https://developer.qiniu.com/censor/api/5620/video-censor#4)  
1. **操作需指定数据源，请先[配置数据源](datasource.md)**  
2. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置)  
3. 单次查询一个审核结果请参考[ single 操作](single.md)  
4. 交互式操作随时输入 id 进行查询请参考[ interactive 操作](interactive.md)  

## 配置
> config.txt
```
path=
process=censorresult
ak=
sk=
id-index=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=censorresult| 查询censor 内容审核结果时设置为censorresult| 表示查询 censor 内容审核结果操作|  
|ak、sk|长度 40 的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取|  
|id-index| 字符串| 转码结果查询所需 jobId 的索引（下标），未设置任何索引时根据 parse 类型默认为 0 或 "id"|  

运行参数：`-config=config.txt`

### 命令行方式
```
-path= -process=censorresult -id-index=
```

## 备注
censorresult 操作是 file 源下的操作，从 every line of fileinput 的 id-index 索引获取 jobId，参见[关于 parse 和索引](datasource.md#关于-parse)。
