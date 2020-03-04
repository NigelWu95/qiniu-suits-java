# 查询域名

## 简介
查询存储空间绑定的域名。该操作是一个以单次运行为主的简单操作，目的是无需登录控制台即可拿到空间绑定的域名，包括以存储空间作为源站的 CDN 域名，以便做一
些其他需要 domain 参数的操作，如导出 url 等。  

1. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置)  

## 使用方式
运行参数：  
```
domainsfrom=<bucket> ak=<ak> sk=<sk>
```  
如使用 qsuits 执行器时：`qsuits -domainsfrom=temp -ak=xxx -sk=xxx`，ak、sk 是必须的要用于七牛鉴权，也可以使用 account 的方式来查询，如：
`qsuits -domainsfrom=temp -a=myself`，会输出对应账号下 temp 空间绑定的所有域名。  

另外，该操作其实也支持批量查询很多空间的绑定域名，但一般不需要这样做，如果希望这样做的话数据源**只能使用本地的文件列表**，即多个 bucket 名称以换行
形式存在于 text 文件列表中，如 bucket 列表：  
```
temp1
temp2
temp3
temp4
temp5
```  
列表文件名为 buckets.txt，那么可以使用如下配置：  
```
process=domainsofbucket
ak=
sk=
```  
同样 ak、sk 也可以使用 account 的方式。如果是运行交互式的命令行依次查询多个 bucket，则命令行加上 `-i` 参数即可，然后输入一个 bucket 名回车一
次，即可依次查询输入 bucket 的域名。  
