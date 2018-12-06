# 空间资源列举

# 简介
列举指定的七牛 bucket 中的文件，支持初始化列举参数指定，支持并发列举和直接列举。支持自定义列举结果的信息字段，得到  
的结果作为数据源进行下一步处理。  

### 配置文件选项
```
ak=
sk=
bucket=
multi=true
max-threads=100
version=2
level=1
unit-len=10000
prefix=
anti-prefix=|
sava-total=true
```
`ak, sk` 表示账号的密钥对字符串  
`bucket` 空间名称  
`multi` 表示是否开启并发列举 (默认开启)  
`max-threads` 表示线程数  
`version` 表示列举接口版本 (1 或 2, 默认为 2)   
`level` 表示列举并发级别 (1 或 2, 默认为 1)  
`unit-len` 表示每次列举请求列举的文件个数  
`prefix` 表示只列举某个文件名前缀的资源  
`anti-prefix` 表示不列举某个文件名前缀的资源，支持以 `,` 分隔的列表  
`sava-total` 表示是否保存完整的原始列举结果  

### 命令行方式
```
-ak= -sk= -bucket= -multi= -max-threads= -version= -level= -unit-len= -prefix= -anti-prefix= -sava-total=true
```

### 关于并发列举
使用前缀索引为每一个前缀 (第一级默认为 94 个连贯的 ASCII 常见字符) 创建一个列举对象，每个列举对象可以列举直到结束，  
在获取多个有效的列举对象之后，分别加入起始（无前缀，但到第一个前缀结束）列举对象和修改终止对象的前缀，随机开始并  
发执行列举，分别对输出结果进行后续处理。前缀索引个数和起始与终止列举对象的前缀会随自定义参数 prefix 和 anti-prefix而  
改变。

* 列举记录，spent time 为列举（或者同时进行 process 的操作）所花费的时间，running threads 为线程数。  

|version|level|unit-len| process |  filter  | file counts |spent time| machine | running threads |  
|-------|-----|--------|---------|----------|-------------|----------|---------|-----------------|  
|   2   |  2  |  10000 |  null   |  false   |  94898690   |   2h18m  | 16核32G |      50         |
|   2   |  1  |  10000 |  null   |  false   |  1893275    |  7minxxs | 8核16G  |      16         | 
|   2   |  2  |  20000 |  null   |  false   |  293940625  |   1h8m   | 16核32G |      200        |
|   2   |  1  |  20000 |  null   |  false   |  1526657    |  5minxxs | 8核16G  |      4          |
|   2   |  1  |  10000 |  null   |  false   |  911559     |  2minxxs | 8核16G  |      15         |

### multi list suggestions
```
1、空间有大量删除时使用 list v1 可能会超时，而且单次请求最大 unit-len 为 1000，直接使用 list v2，即 version=2，实际情况也  
   是 list v2 效率更高，建议默认使用 2。
2、推荐用法：version=2，unit-len=20000（version 2 的时候 unit-len 值可以调高，但是不建议过大，通常不超过 100000），500  
   万以内文件 level=1，500 万以上文件 level=2，max-threads=100（在机器配置较高时可以选择更高的值，如16核32G的机器可选择  
   200 个以上线程）。
```
