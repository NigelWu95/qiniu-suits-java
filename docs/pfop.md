# 资源数据处理

# 简介
对空间中的资源请求 pfop 持久化数据处理。参考：[七牛数据处理 pfop 文档](https://developer.qiniu.com/dora/manual/3686/pfop-directions-for-use)

### 配置文件选项

#### 必须参数
|数据源方式|参数及赋值|  
|--------|-----|  
|source-type=file（文件资源列表）|[file 数据源参数](fileinput.md) <br> process=pfop <br> ak=\<ak\> <br> sk=\<sk\> <br> bucket=\<bucket\> <br> pipeline=\<pipeline\> |  

#### 可选参数
```
force-public=
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=pfop| 数据处理时设置为pfop| 表示数据处理操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源方式为 list 时无需再设置|  
|bucket| 字符串| 操作的资源原空间，当数据源为 list 时无需再设置|  
|pipeline| 字符串| 进行持久化数据处理的队列名称|  
|force-public| true/false| 是否强制使用共有队列（会有性能影响）|  

### 命令行方式
```
-process=pfop -ak= -sk= -bucket= -pipeline= -force-public=
```
