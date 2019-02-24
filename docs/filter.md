# 资源过滤

# 简介
对资源信息进行过滤，作为一个 process 过程来处理，接收 source-type 数据源输入的信息字段按
条件过滤后输出符合条件的记录，目前支持七牛存储资源的元信息字段。  

### 配置文件选项
```
f-prefix=
f-suffix=
f-regex=
f-mime=image
f-type=
f-status=
f-date=2018-08-01
f-time=00:00:00
f-direction=1
f-anti-prefix=
f-anti-suffix=
f-anti-regex=
f-anti-mime=
```
`f-prefix` 表示**选择**文件名符合该前缀的文件  
`f-suffix` 表示**选择**文件名符合该后缀的文件  
`f-inner` 表示**选择**文件名包含该部分字符的文件  
`f-regex` 表示**选择**文件名符合该正则表达式的文件，所填内容必须为正则表达式  
`f-mime` 表示**选择**符合该 mime 类型的文件  
`f-type` 表示**选择**符合该存储类型的文件, 为 0（） 或 1  
`f-status` 表示**选择**符合该存储状态的文件, 为 0 或 1  
`f-date, f-time` 设置过滤的时间节点  
`f-direction` 表示时间节点过滤方向，0 表示选择**时间点以前**更新的文件，1 表示选择**时间点以后**更新的文件  
`f-anti-prefix` 表示**排除**文件名符合该前缀的文件  
`f-anti-suffix` 表示**排除**文件名符合该后缀的文件  
`f-anti-inner` 表示**排除**文件名包含该部分字符的文件  
`f-anti-regex` 表示**排除**文件名符合该正则表达式的文件，所填内容必须为正则表达式  
`f-anti-mime` 表示**排除**该 mime 类型的文件  

**备注：**  
过滤条件中，f-prefix,f-suffix,f-inner,f-regex,f-mime 可以为列表形式，用逗号分割，如 param1,param2,param3。
f-prefix,f-suffix,f-inner,f-regex 四个均为针对文件名 key 的过滤条件，多个过滤条件时使用 &&（与）的关系得到最终过滤结果。f-anti-xx 的参数
表示反向过滤条件，即排除符合该特征的记录。

### 命令行方式
```
-f-prefix= -f-suffix= -f-inner= -f-regex= -f-mime= -f-type= -f-status= -f-date= -f-time= -f-direction= -f-anti-prefix= -f-anti-suffix= -f-anti-inner= -f-anti-regex= -f-anti-mime=
```
