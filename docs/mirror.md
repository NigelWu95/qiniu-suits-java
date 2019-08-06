# 资源镜像更新

## 简介
对空间中的资源进行镜像源更新。参考：[七牛空间资源镜像更新](https://developer.qiniu.com/kodo/api/1293/prefetch)

## 配置文件
**操作需指定数据源，请先[配置数据源](datasource.md)**  

### 配置参数
```
process=mirror
ak=
sk=
bucket=
indexes=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=mirror| 资源镜像更新时设置为mirror| 表示资源镜像更新操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源为 qiniu 时无需再设置|  
|bucket| 字符串| 镜像源空间，当数据源为 qiniu 时无需再设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  

## 命令行方式
```
-process=mirror -ak= -sk= -bucket=  
```
