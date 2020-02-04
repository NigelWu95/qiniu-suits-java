version=8.3.14

package_no_test:
	mvn package -Dmaven.test.skip=true

deploy_no_test:
	mvn clean deploy -P release -Dmaven.test.skip=true

release_jar_to_devtools:
	qsuits -s -path=target/qsuits-$(version)-jar-with-dependencies.jar -process=qupload -a=devtools -bucket=devtools -keep-path=false
	qsuits -s -url=http://devtools.qiniu.com/qsuits-$(version)-jar-with-dependencies.jar -process=cdnprefetch -a=devtools
	qsuits -s -url=https://devtools.qiniu.com/qsuits-$(version)-jar-with-dependencies.jar -process=cdnprefetch -a=devtools

clean_logs:
	rm -rf logs*

clean:
	mvn clean

build:
	mvn package -Dmaven.test.skip=true
	[ -d ~/.qsuits ] || mkdir ~/.qsuits
	cp target/qsuits-$(version)-jar-with-dependencies.jar ~/.qsuits/qsuits-$(version).jar
