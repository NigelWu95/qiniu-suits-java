# 空间资源列举

# 简介
列举指定的七牛 bucket 中的文件，支持初始化列举参数指定，支持并发列举和直接列举。支持自定
义列举结果的信息字段，得到的结果作为数据源进行下一步处理。参考：[七牛资源列举](https://developer.qiniu.com/kodo/api/1284/list)  

#### 必须参数
```
source-type=list (v.2.11 及以上版本也可以使用 source=file，或者不设置该参数)
ak=
sk=
bucket=
```

#### 可选参数
```
threads=100
unit-len=10000
prefixes=
anti-prefixes=
prefix-left=
prefix-right=
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|source-type/source| 资源列举时设置为list | 表示从目标空间中列举出资源|  
|ak、sk|长度 40 的字符串|七牛账号的密钥对字符串，通过七牛控制台个人中心获取|  
|bucket|字符串| 需要列举的空间名称|  
|threads| 整型数字| 表示并发列举时使用的线程数（默认 30）|  
|unit-len| 整型数字| 表示每次列举请求列举的文件个数（列举长度，默认值 10000）|  
|prefixes| 字符串| 表示只列举某些文件名前缀的资源，，支持以 `,` 分隔的列表|  
|anti-prefixes| 字符串| 表示列举时排除某些文件名前缀的资源，支持以 `,` 分隔的列表|  
|prefix-left| true/false| 当设置 prefixes 时，可选择是否在prefixes ASCII顺序之前的所有未知前缀的文件是否进行列举|  
|prefix-right| true/false| 当设置 prefixes 时，可选择是否在prefixes ASCII顺序之后的所有未知前缀的文件是否进行列举|  

### 命令行参数方式
```
-ak= -sk= -bucket= -threads= -unit-len= -prefixes= -anti-prefixes= -prefix-left= -prefix-right=
```

### 关于并发列举
1、算法描述：使用前缀索引为每一个前缀 (第一级默认为连贯的 ASCII 常见字符) 创建一个列举对象，每个列
举对象可以列举直到结束，在获取多个有效的列举对象之后，分别加入起始（无前缀，但到第一个前缀结束）列举对象和修
改终止对象的前缀，随即开始并发执行列举，分别对输出结果进行后续处理。前缀索引个数和起始与终止列举对象的前缀会
随自定义参数 prefixes 和 anti-prefixes 而改变，prefixes 为指定列举的公共前缀，anti-prefixes 表示
从前缀索引中去除的索引字符，通常 prefixes 和 anti-prefixes 不同时进行设置。  

2、大量文件时建议：threads=100, unit-len=10000，unit-len 值在机器配置较高时可以调高，如16核32G的机
器可选择 200 个以上线程，但是不建议过大，通常不超过 100000。500 万以内文件建议 threads<=100，文件数目
较少时不建议使用较多线程，如 100 万左右及以下的文件建议线程数少于 50，甚至更少文件时可使用单线程直接列举：
threads=1。（文件数较少时若设置并发线程数偏多则会增加额外耗时）
