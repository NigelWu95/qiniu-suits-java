# 资源数据处理

## 简介
对空间中的资源请求 pfop 持久化数据处理。参考：[七牛数据处理 pfop 文档](https://developer.qiniu.com/dora/manual/3686/pfop-directions-for-use)

## 配置文件选项

### 配置参数
```
process=pfop 
ak= 
sk= 
bucket=
pipeline=
fops-index=
force-public=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=pfop| 数据处理时设置为pfop| 表示数据处理操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源方式为 list 时无需再设置|  
|bucket| 字符串| 操作的资源原空间，当数据源为 list 时无需再设置|  
|pipeline| 字符串| 进行持久化数据处理的队列名称|  
|force-public| true/false| 是否强制使用共有队列（会有性能影响）|  
|fops-index| 字符串| 转码命令索引（下标），pfop 操作事必须指定|  

### 关于 fops-index
指定输入行中对应转码的命令字段下标，不设置为则无法进行解析。由于转码必须参数包含 key 和 fops，因此输入行中也必须包含 key 字段的值，使用 indexes 
参数设置 key 下标，同时 key 下标不能与 fops 下标相同。

## 命令行方式
```
-process=pfop -ak= -sk= -bucket= -pipeline= -force-public=
```
