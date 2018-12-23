# 空间资源列举

# 简介
列举指定的七牛 bucket 中的文件，支持初始化列举参数指定，支持并发列举和直接列举。支持自定
义列举结果的信息字段，得到的结果作为数据源进行下一步处理。参考：[七牛资源列举](https://developer.qiniu.com/kodo/api/1284/list)  

#### 必须参数
```
source-type=list
ak=
sk=
bucket=
```

#### 可选参数
```
multi=true
threads=100
unit-len=10000
prefix=
anti-prefix=
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|source-type| 资源列举时设置为list | 表示从目标空间中列举出资源|  
|ak、sk|长度 40 的字符串|七牛账号的密钥对字符串，通过七牛控制台个人中心获取|  
|bucket|字符串| 需要列举的空间名称|  
|multi| true/false| 表示是否开启并发列举 (默认开启)|  
|threads| 整型数字| 表示并发列举时使用的线程数（默认 30）|  
|unit-len| 整型数字| 表示每次列举请求列举的文件个数（列举长度，默认值 10000）|  
|prefix| 域名字符串| 表示只列举某个文件名前缀的资源|  
|anti-prefix| true/false| 表示列举时排除某个文件名前缀的资源，支持以 `,` 分隔的列表|  

### 命令行参数方式
```
-ak= -sk= -bucket= -multi= -threads= -unit-len= -prefix= -anti-prefix=
```

### 关于并发列举
使用前缀索引为每一个前缀 (第一级默认为 94 个连贯的 ASCII 常见字符) 创建一个列举对象，每个列举对
象可以列举直到结束，在获取多个有效的列举对象之后，分别加入起始（无前缀，但到第一个前缀结束）
列举对象和修改终止对象的前缀，随即开始并发执行列举，分别对输出结果进行后续处理。前缀索引个数
和起始与终止列举对象的前缀会随自定义参数 prefix 和 anti-prefix 而改变。

* 列举记录，spent time 为列举（或者同时进行 process 的操作）所花费的时间，running threads 为线程数。  

|unit-len| process |  filter  | file counts |spent time| machine | running threads |  
|--------|---------|----------|-------------|----------|---------|-----------------|  
|  10000 |  null   |  false   |  94898690   |   2h18m  | 16核32G |      50         |
|  10000 |  null   |  false   |  1893275    |  7minxxs | 8核16G  |      16         | 
|  20000 |  null   |  false   |  293940625  |   1h8m   | 16核32G |      200        |
|  20000 |  null   |  false   |  1526657    |  5minxxs | 8核16G  |      4          |
|  10000 |  null   |  false   |  911559     |  2minxxs | 8核16G  |      15         |

### multi list suggestions
```
1、大量文件时建议：multi=true（true 为默认值）, threads=100, unit-len=10000，unit-len 值在机器配
置较高时可以调高，如16核32G的机器可选择 200 个以上线程，但是不建议过大，通常不超过 100000。500 万以内文
件建议 threads<=100，100 万以内文件数建议不启用并发列举，选择：multi=false, threads=100。（文件数较
少时若设置并发线程数偏多则会增加额外耗时）
```
