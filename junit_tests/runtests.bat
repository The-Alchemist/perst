if not defined ANT_HOME set ANT_HOME=\apache-ant-1.6.5
if not defined JAVA_HOME set JAVA_HOME=\j2sdk1.5.0
if not defined JUNIT_JAR set JUNIT_JAR=lib\junit-3.8.1.jar

set saveclasspath=%classpath%
set savepath=%path%
set path=%ANT_HOME%\bin;%JAVA_HOME%\bin;%path%
set classpath=%JUNIT_JAR%;%classpath%
call ant run-test
set path=%savepath%
set classpath=%classsavepath%
