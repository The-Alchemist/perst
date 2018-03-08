if not defined LUCENE_JAR set LUCENE_JAR=c:\lucene-2.4.1\lucene-core-2.4.1.jar

javac -g -classpath %LUCENE_JAR%;..\lib\perst.jar *.java