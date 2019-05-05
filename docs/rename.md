# 资源重命名

## 简介
对空间中的资源进行**重命名**。参考：[七牛空间资源重命名](https://developer.qiniu.com/kodo/api/1288/move)/[批量重命名](https://developer.qiniu.com/kodo/api/1250/batch)

## 配置文件选项
**操作通常需要指定数据源，默认表示从七牛空间列举文件执行操作，如非默认或需更多条件，请先[配置数据源](../docs/datasource.md)**  

### 配置参数
```
ak=
sk=
bucket= 
newKey-index=
add-prefix=
rm-prefix=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=rename| 资源重命名时设置为rename| 表示重命名操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源为 qiniu 时无需再设置|  
|bucket| 字符串| 操作的资源原空间，当数据源为 qiniu 时无需再设置|  
|newKey-index| 字符串| rename 操作所需要设置的目标文件名索引（下标），需要手动指定才会进行解析|  
|add-prefix| 字符串| 表示为保存的文件名添加指定前缀|  
|rm-prefix| 字符串| 表示将原文件名去除存在的指定前缀后作为 rename 之后保存的文件名|  
|prefix-force| 字符串| 设置了 add-prefix 但未设置 newKey-index 的情况下需要保证该参数为 true|  

### 关于 newKey-index
指定输入行中对应修改之后的文件名字段下标，不设置则无法进行解析，当使用 file 源且 parse=tab/csv 时下标必须为整数，但未设置且 add-prefix 不为空
时需要强制指定 prefix-force=true，表明该次重命名操作只添加文件名前缀。由于 rename 操作既需要原始文件名字段也需要新文件名字段，因此 newKey 下
标和 key 字段下标不可相同，key 下标用 indexes 参数设置，。  

## 命令行方式
```
-process=rename -ak= -sk= -bucket= -newKey-index= -add-prefix= -rm-prefix= -prefix-force=
```
