# 资源更新存储类型

# 简介
对空间中的资源进行修改存储类型。参考：[七牛空间资源更新存储类型](https://developer.qiniu.com/kodo/api/3710/chtype)/[批量更新存储类型](https://developer.qiniu.com/kodo/api/1250/batch)

### 配置文件选项

#### 必须参数
|数据源方式|参数及赋值|  
|--------|-----|  
|source-type=list（空间资源列举）|[list 数据源参数](listbucket.md) <br> process=type <br> type=\<type\> |  
|source-type=file（文件资源列表）|[file 数据源参数](fileinput.md) <br> process=type <br> ak=\<ak\> <br> sk=\<sk\> <br> bucket=\<bucket\> <br> type=\<type\> |  

#### 可选参数
```
ak=
sk=
bucket= 
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=type| 更新资源存储类型时设置为type| 表示更新存储类型操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源方式为 list 时无需再设置|  
|bucket| 字符串| 操作的资源原空间，当数据源为 list 时无需再设置|  
|type| 0/1| 设置资源的存储类型为 type，0 表示标准存储，1 表示低频存储|  

### 命令行方式
```
-process=type -ak= -sk= -bucket= -type=  
```
