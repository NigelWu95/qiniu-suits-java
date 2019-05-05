# 资源更新生命周期

## 简介
对空间中的资源修改生命周期。参考：[七牛空间资源更新生命周期](https://developer.qiniu.com/kodo/api/1732/update-file-lifecycle)/[批量更新生命周期](https://developer.qiniu.com/kodo/api/1250/batch)

## 配置文件选项
**操作通常需要指定数据源，默认表示从七牛空间列举文件执行操作，如非默认或需更多条件，请先[配置数据源](../docs/datasource.md)**  

### 配置参数
```
process=
ak=
sk=
bucket= 
days=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=lifecycle| 更新资源生命周期时设置为lifecycle| 表示更新生命周期操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源为 qiniu 时无需再设置|  
|bucket| 字符串| 操作的资源原空间，当数据源为 qiniu 时无需再设置|  
|days| 整型数字| 设置资源的生命周期为 days（单位天数），为 0 时表示永久的生命周期|  

## 命令行方式
```
-process=lifecycle -ak= -sk= -bucket= -days=  
```
