set SAVE_PATH=%PATH%
set PATH=\Program Files\Java\jdk1.5.0_06\bin;%JAVA_HOME%\bin;%path%
if not defined LUCENE_JAR set LUCENE_JAR=c:\lucene-2.4.1\lucene-core-2.4.1.jar
javac -Xlint:deprecation -source 1.5 -classpath ../../lib/perst.jar;%LUCENE_JAR% -g org/garret/perst/continuous/*.java
jar cvf ../../lib/continuous.jar org/garret/perst/continuous/*.class
set PATH=%SAVE_PATH%