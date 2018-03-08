set SAVE_PATH=%PATH%
set PATH=\Program Files\Java\jdk1.5.0_06\bin;%JAVA_HOME%\bin;%path%
if not defined LUCENE_JAR set LUCENE_JAR=c:\lucene-2.3.2\build\lucene-core-2.3.2.jar;\lucene-2.1.0\lucene-core-2.1.0.jar
java -classpath .;../../lib/perst.jar;../../lib/continuous.jar;%LUCENE_JAR% Bank
set PATH=%SAVE_PATH%