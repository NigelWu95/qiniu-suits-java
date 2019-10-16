# 资源更新存储类型

## 简介
对空间中的资源进行修改存储类型。参考：[七牛空间资源更新存储类型](https://developer.qiniu.com/kodo/api/3710/chtype)/[批量更新存储类型](https://developer.qiniu.com/kodo/api/1250/batch)  
1. **操作需要指定数据源，默认表示从七牛空间列举文件执行操作，如非默认或需更多条件，请先[配置数据源](datasource.md)**  
2. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置)  

## 配置文件

### 配置参数
```
process=type
ak=
sk=
bucket=
indexes=
type=
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=type| 更新资源存储类型时设置为type| 表示更新存储类型操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源为 qiniu 时无需再设置|  
|bucket| 字符串| 操作的资源原空间，当数据源为 qiniu 时无需再设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|type| 0/1| 设置资源的存储类型为 type，0 表示标准存储，1 表示低频存储|  

## 命令行方式
```
-process=type -ak= -sk= -bucket= -type=  
```
