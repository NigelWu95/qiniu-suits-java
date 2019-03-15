# 资源镜像更新

## 简介
对空间中的资源进行镜像源更新。参考：[七牛空间资源镜像更新](https://developer.qiniu.com/kodo/api/1293/prefetch)

## 配置文件选项

### 配置参数
```
process=mirror  
ak=
sk=
bucket=
rm-prefix=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=mirror| 资源镜像更新时设置为mirror| 表示资源镜像更新操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源方式为 list 时无需再设置|  
|bucket| 字符串| 操作的资源所在空间，当数据源为 list 时无需再设置|  
|rm-prefix| 字符串| 表示将得到的目标文件名去除存在的指定前缀后再进行 mirror 更新，用于输入的文件名可能比实际空间的文件名多了前缀的情况|  

## 命令行方式
```
-process=mirror -ak= -sk= -bucket=  
```
