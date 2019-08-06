# 资源移动

## 简介
对空间中的资源进行**移动**到另一目标空间。参考：[七牛空间资源移动](https://developer.qiniu.com/kodo/api/1288/move)/[批量移动](https://developer.qiniu.com/kodo/api/1250/batch)

## 配置文件
**操作通常需要指定数据源，默认表示从七牛空间列举文件执行操作，如非默认或需更多条件，请先[配置数据源](datasource.md)**  

### 配置参数
```
process=move
ak=
sk=
bucket= 
to-bucket=
toKey-index=
add-prefix=
rm-prefix=
prefix-force=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=move| 移动资源时设置为move| 表示移动操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源为 qiniu 时无需再设置|  
|bucket| 字符串| 操作的资源原空间，当数据源为 qiniu 时无需再设置|  
|to-bucket| 字符串| 移动资源保存的目标空间|  
|toKey-index| 字符串| move 操作所需要设置的目标文件名索引（下标），需要手动指定才会进行解析|  
|add-prefix| 字符串| 表示为保存的文件名添加指定前缀|  
|rm-prefix| 字符串| 表示将原文件名去除存在的指定前缀后作为 move 之后保存的文件名|  

### 命令行方式
```
-process=move -ak= -sk= -bucket= -to-bucket= -add-prefix= -rm-prefix=
```
