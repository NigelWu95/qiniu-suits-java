# 资源更新状态

# 简介
对空间中的资源修改状态。参考：[七牛空间资源更新状态](https://developer.qiniu.com/kodo/api/4173/modify-the-file-status)/[批量更新状态](https://developer.qiniu.com/kodo/api/1250/batch)

### 配置文件选项

#### 必须参数
|数据源方式|参数及赋值|  
|--------|-----|  
|source-type=list（空间资源列举）|[list 数据源参数](listbucket.md) <br> process=status <br> status=\<status\> |  
|source-type=file（文件资源列表）|[file 数据源参数](fileinput.md) <br> process=status <br> ak=\<ak\> <br> sk=\<sk\> <br> bucket=\<bucket\> <br> status=\<status\> |  

#### 可选参数
```
ak=
sk=
bucket= 
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=status| 更新资源状态时设置为status| 表示更新状态操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源方式为 list 时无需再设置|  
|bucket| 字符串| 操作的资源原空间，当数据源为 list 时无需再设置|  
|status| 0/1| 设置资源的状态为 type，0表示文件启用，1 表示文件禁用|  

### 命令行方式
```
-process=status -ak= -sk= -bucket= -status=  
```
