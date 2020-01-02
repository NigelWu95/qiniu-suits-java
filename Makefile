version=8.3.12

package_no_test:
	mvn package -Dmaven.test.skip=true

deploy_no_test:
	mvn clean deploy -P release -Dmaven.test.skip=true

release_jar_to_devtools:
	qsuits -s -path=target/qsuits-$(version)-jar-with-dependencies.jar -process=qupload -a=devtools -bucket=devtools -keep-path=false

clean_logs:
	rm -rf logs*