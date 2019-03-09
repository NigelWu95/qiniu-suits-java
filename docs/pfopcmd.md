# 生成音视频转码指令

## 简介
根据已经获取到的音视频资源的 avinfo 信息，来设置规则判断进行某种自定义的转码并自定义转码后的文件名。参考：[七牛数据处理 pfop 文档](https://developer.qiniu.com/dora/manual/3686/pfop-directions-for-use)

## 配置文件选项

### 配置参数
```
process=pfopcmd 
pfop-config=
duration=
size=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=pfopcmd| 该操作设置为pfopcmd| 表示根据 avinfo 生成音视频转码指令|  
|pfop-config| 文件路径字符串| 进行转码和另存规则设置的配置文件路径，配置文件格式为 json，用于设置多个转码条件和指令，配置举例：[pfop-config 配置](../resources/pfop.json)|  
|duration| true/false| 得到的结果行中是否需要保存 duration（音视频时长）信息，会放在转码指令字段之后 |  
|size| true/false| 得到的结果行中是否需要保存 size（音视频时长）信息，会放在 duration 字段之后|  

#### # pfop-config 配置文件写法如下：
```
{
  "F720":{
    "scale":[1000,1279],
    "cmd":"avthumb/mp4/s/1280x720/autoscale/1",
    "saveas":"bucket:$(key)F720.mp4"
  }
}
```
|必须选项|含义|  
|-----|-----|  
|key|上述配置文件中的 "F720" 为转码项名称，设置为 json key，key 不可重复，重复情况下后者会覆盖前者|  
|scale| 表示 width 的范围，只设置了一个值则表示目标范围大于该值，程序根据 avinfo 判断宽度范围在此区间，才针对文件名生成转码指令，否则跳过即不进行该规则的转码|  
|cmd| 需要指定的转码指令 |  
|saveas| 转码结果另存的格式，写法为："<bucket>:<key>"，其中 <key> 支持变量 $(key) 表示这一部分为原文件名|  

**关于 saveas**： 
变量 $(key) 在 saveas 中没有设置后缀名的情况下会保持原文件名的后缀名，明确设置了后缀名的话另存的文件名会使用该后缀。如原文件名 key=12345.mov，
设置 "saveas":"bucket:$(key)F1080"，则会得到另存的结果为 "bucket:12345F1080.mov"，如果设置 "saveas":"bucket:$(key)F1080.mp4"，则
会得到另存的结果为 "bucket:12345F1080.mp4"。  

## 命令行方式
```
-process=pfopcmd -pfop-config= -duration= -size=
```
