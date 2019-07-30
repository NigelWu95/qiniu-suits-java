# 资源删除

## 简介
对空间中的资源进行删除。参考：[七牛空间资源删除](https://developer.qiniu.com/kodo/api/1257/delete)/[批量删除](https://developer.qiniu.com/kodo/api/1250/batch)

## 配置文件
**操作通常需要指定数据源，默认表示从七牛空间列举文件执行操作，如非默认或需更多条件，请先[配置数据源](../docs/datasource.md)**  

### 配置参数
```
process=delete
ak=
sk=
bucket=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=delete| 删除资源时设置为delete| 表示资源删除操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源方式为 qiniu 时无需再设置|  
|bucket| 字符串| 操作的资源所在空间，当数据源为 qiniu 时无需再设置|  

## 命令行方式
```
-process=delete -ak= -sk= -bucket=  
```
