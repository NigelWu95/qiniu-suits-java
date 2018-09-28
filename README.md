# qiniu-java-suits
七牛接口使用套件

### command

* list bucket and process per item
```
java -jar qiniu-java-suits -ak= -sk= -bucket= -result-path=../result -max-threads=30 -version=2 -end-file=true -parallel=true -level=2 -process=copy -unit-len=1000 -type=1 -status=0 -date=2018-08-01 -time=00:00:00 -direction=0 -access-key= -secret-key= -from= -to= -keep-key=true -add-prefix=
```
