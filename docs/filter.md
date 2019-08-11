# 资源过滤

## 简介
基于[默认的基础字段](datasource.md#关于文件信息字段)对资源信息进行过滤，接受数据源输入的信息字段按条件过滤后输出符合条件的记录，目前支持七牛存
储资源的信息字段。  

## 配置文件
**操作需指定数据源，请先[配置数据源](datasource.md)**  

### 配置参数
```
f-prefix=
f-suffix=
f-regex=
f-mime=image
f-type=
f-status=
f-date-scale=
f-anti-prefix=
f-anti-suffix=
f-anti-regex=
f-anti-mime=
f-check=
f-check-config=
f-check-rewrite=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|f-prefix| ","分隔的字符串列表| 表示**选择**文件名符合该前缀的文件|  
|f-suffix| ","分隔的字符串列表| 表示**选择**文件名符合该后缀的文件|  
|f-inner| ","分隔的字符串列表| 表示**选择**文件名包含该部分字符的文件|  
|f-regex| ","分隔的字符串列表| 表示**选择**文件名符合该正则表达式的文件，所填内容必须为正则表达式|  
|f-mime| ","分隔的字符串列表| 表示**选择**符合该 mime 类型的文件|  
|f-type| 存储类型值| 表示**选择**符合该存储类型的文件，参考[关于 f-type](#关于-f-type)|  
|f-status| 0/1| 表示**选择**符合该存储状态的文件, 为 0（启用）或 1（禁用）|  
|f-date-scale| 字符串| 设置过滤的时间范围，格式为 [\<date1\>,\<date2\>]，\<date\> 格式为：2018-08-01 00:00:00，[特殊规则](#f-date-scale)|  
|f-anti-prefix| ","分隔的字符串列表| 表示**排除**文件名符合该前缀的文件|  
|f-anti-suffix| ","分隔的字符串列表| 表示**排除**文件名符合该后缀的文件|  
|f-anti-inner| ","分隔的字符串列表| 表示**排除**文件名包含该部分字符的文件|  
|f-anti-regex| ","分隔的字符串列表| 表示**排除**文件名符合该正则表达式的文件，所填内容必须为正则表达式|  
|f-anti-mime| ","分隔的字符串列表| 表示**排除**该 mime 类型的文件|  
|f-check|字符串| 是否进行字段关联匹配性检查，不符合规范的疑似异常文件将被筛选出来|  
|f-check-config|配置文件路径字符串|自定义资源字段规范对应关系列表的配置文件，格式为 json|  
|f-check-rewrite|true/false|是否完全使用自定义的规范列表进行检查，默认为 false|  

#### 关于 f-type
|存储源|type 参数类型|具体值                   |
|-----|-----------|------------------------|
|七牛  | 整型      |0 表示标准存储；1 表示低频存储|
|其他  | 字符串     |如：Standard 表示标准存储，IA 表示低频存储，Archive 表示归档存储 |  

#### 特殊字符
特殊字符包括: `, \ =` 如有参数值本身包含特殊字符需要进行转义：`\, \\ \=`  

#### f-date-scale
<date> 中的 00:00:00 为默认值可省略，无起始时间则可填 [0,\<date2\>]，结束时间支持 now 和 max，分别表示到当前时间为结束或无结束时间。  

#### 基本字段过滤  
过滤条件中，f-prefix,f-suffix,f-inner,f-regex,f-mime 可以为列表形式，用逗号分割，如 param1,param2,param3。
f-prefix,f-suffix,f-inner,f-regex 四个均为针对文件名 key 的过滤条件，多个过滤条件时使用 &&（与）的关系得到最终过滤结果。f-anti-xx 的参数
表示反向过滤条件，即排除符合该特征的记录。  

#### 特殊特征匹配过滤 f-check[-x]  
根据资源的字段关系选择某个特征下的文件，目前支持 "ext-mime" 检查，程序内置的默认特征配置见：[check 默认配置](../resources/check.json)，运行
参数选项如下：  
`f-check=ext-mime` 表示进行**后缀名 ext **和**mimeType**（即 content-type）匹配性检查，不符合规范的疑似异常文件将被筛选出来  
`f-check-config` 自定义资源字段规范对应关系列表的配置文件，格式为 json，自定义规范配置 key 字段必填，其元素类型为列表 [], 否则无效，如
"ext-mime" 配置时后缀名和 mimeType 用 ":" 组合成字符串成为一组对应关系，写法如下：  
```
{
  "ext-mime": [
    "mp5:video/mp5"
  ]
}
```  
配置举例：[check-config 配置](../resources/check-config.json)  
`f-check-rewrite` 是否覆盖默认的特征配置，为 false（默认）表示将自定义的规范对应关系列表和默认的列表进行叠加，否则程序内置的规范对应关系将失效，
只检查自定义的规范列表。  

## 命令行方式
```
-f-prefix= -f-suffix= -f-inner= -f-regex= -f-mime= -f-type= -f-status= -f-date-scale= -f-anti-prefix= -f-anti-suffix= -f-anti-inner= -f-anti-regex= -f-anti-mime=
```
