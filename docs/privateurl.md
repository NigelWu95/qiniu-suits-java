# 私有空间资源签名

## 简介
对七牛私有空间中的资源进行签名。参考：[七牛私有空间资源签名](https://developer.qiniu.com/kodo/manual/1656/download-private)  

## 配置文件选项

### 配置参数
```
process=privateurl 
ak=
sk=
domain=
protocol=
url-index=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process| 私有资源签名时设置为privateurl | 表示私有资源生成签名链接操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源方式为 list 时无需再设置|  
|domain| 域名字符串| 用于拼接文件名生成链接的域名，数据源为 file 且指定 url-index 时无需设置|  
|protocol| http/https| 使用 http 还是 https 访问资源进行抓取（默认 http）|  
|url-index| 字符串| 通过 url 操作时需要设置的 url 索引（下标），需要手动指定才会进行解析|  

### 关于 url-index
当 parse=tab/csv 时下标必须为整数。url-index 表示输入行中存在 url 形式的源文件地址，未设置的情况下则默认从 key 字段加上 domain 的方式访问源
文件地址，key 下标用 indexes 参数设置。  

## 命令行方式
```
-process=avinfo -ak= -sk= -domain= -protocol= 
```
