# 资源重命名

# 简介
对空间中的资源进行**重命名**。参考：[七牛空间资源重命名](https://developer.qiniu.com/kodo/api/1288/move)/[批量重命名](https://developer.qiniu.com/kodo/api/1250/batch)

### 配置文件选项

#### 必须参数
|数据源方式|参数及赋值|  
|--------|-----|  
|source-type=list（空间资源列举）|[list 数据源参数](listbucket.md) <br> process=rename |  
|source-type=file（文件资源列表）|[file 数据源参数](fileinput.md) <br> process=rename <br> ak=\<ak\> <br> sk=\<sk\> <br> bucket=\<bucket\> |  

#### 可选参数
```
ak=
sk=
bucket= 
newKey-index=1
add-prefix=
rm-prefix=
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=rename| 资源重命名时设置为rename| 表示重命名操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源方式为 list 时无需再设置|  
|bucket| 字符串| 操作的资源原空间，当数据源为 list 时无需再设置|  
|newKey-index| 字符串| rename 操作所需要设置的目标文件名索引（下标），需要手动指定才会进行解析|  
|add-prefix| 字符串| 表示为保存的文件名添加指定前缀|  
|rm-prefix| 字符串| 表示将原文件名去除存在的指定前缀后作为 rename 之后的文件名|  

#### 关于 newKey-index
指定输入行中对应修改之后的文件名字段下标，不设置为则无法进行解析，但当 add-prefix 存在且不为空时需要强制指定 prefix-force=true，表明该次重命名
操作可以只添加文件名前缀。由于 rename 操作几原始文件名字段也需要新文件名字段，因此 newKey 下标和 key字段下标不可相同。  

### 命令行方式
```
-process=rename -ak= -sk= -bucket= -add-prefix=
```
