# 资源复制

# 简介
对空间中的资源进行**复制**到另一个目标空间。参考：[七牛空间资源复制](https://developer.qiniu.com/kodo/api/1254/copy)/[批量复制](https://developer.qiniu.com/kodo/api/1250/batch)

### 配置文件选项

#### 必须参数
|数据源方式|参数及赋值|  
|--------|-----|  
|source-type=list（空间资源列举）|[list 数据源参数](listbucket.md) <br> process=copy <br> to-bucket=\<to-bucket\> |  
|source-type=file（文件资源列表）|[file 数据源参数](fileinput.md) <br> process=copy <br> ak=\<ak\> <br> sk=\<sk\> <br> bucket=\<bucket\> <br> to-bucket=\<to-bucket\> |  

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
|process=copy| 复制资源时设置为copy| 表示复制操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源方式为 list 时无需再设置|  
|bucket| 字符串| 操作的资源原空间，当数据源为 list 时无需再设置|  
|to-bucket| 字符串| 复制资源保存的目标空间|  
|newKey-index| 字符串| copy 操作可选择设置的目标文件名索引（下标），需要手动指定才会进行解析|  
|add-prefix| 字符串| 表示为保存的文件名添加指定前缀|  
|rm-prefix| 字符串| 表示将原文件名去除存在的指定前缀后作为 copy 之后的文件名|  

### 命令行方式
```
-process=copy -ak= -sk= -bucket= -to-bucket= -add-prefix=
```
