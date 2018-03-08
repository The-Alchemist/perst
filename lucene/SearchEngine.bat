if not defined LUCENE_JAR set LUCENE_JAR=c:\lucene-2.4.1\lucene-core-2.4.1.jar

java -Xmx512M -classpath %LUCENE_JAR%;. SearchEngine -create
