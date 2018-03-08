set SAVE_PATH=%PATH%
set PATH=\Program Files\Java\jdk1.5.0_06\bin;%JAVA_HOME%\bin;%path%
if not defined LUCENE_JAR set LUCENE_JAR=\lucene-2.4.1\build\lucene-core-2.1.1-dev.jar;\lucene-2.4.1\lucene-core-2.4.1.jar
javadoc -source 1.5 -d ../doc -classpath ../../lib/perst.jar;%LUCENE_JAR% -nodeprecated -nosince -public org/garret/perst/continuous/*.java
set PATH=%SAVE_PATH%
