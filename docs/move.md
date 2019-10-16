# 资源移动

## 简介
对空间中的资源进行**移动**到另一目标空间。参考：[七牛空间资源移动](https://developer.qiniu.com/kodo/api/1288/move)/[批量移动](https://developer.qiniu.com/kodo/api/1250/batch)  
1. **操作需要指定数据源，默认表示从七牛空间列举文件执行操作，如非默认或需更多条件，请先[配置数据源](datasource.md)**  
2. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置（7.73-及以上版本）)  

## 配置文件

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

### 关于 toKey-index
指定输入行中对应修改之后的文件名字段下标，不设置则无法进行解析，当使用 file 源且 parse=tab/csv 时下标必须为整数，但未设置且 add-prefix 不为空
时需要强制指定 prefix-force=true，表明该次重命名操作只添加文件名前缀。由于 rename 操作既需要原始文件名字段也需要新文件名字段，因此 toKey 下
标和 key 字段下标不可相同，key 下标用 indexes 参数设置，。  
**注意**：七牛存储空间不支持文件名以 `../`, `./` 开头或者包含 `/../`, `/./` 这种情况，会造成无法访问，因此设置文件名时请注意。  

### 命令行方式
```
-process=move -ak= -sk= -bucket= -to-bucket= -add-prefix= -rm-prefix=
```
