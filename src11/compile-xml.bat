set SAVE_PATH=%PATH%
set SAVE_CLASSPATH=%CLASSPATH%
set PATH=c:\jdk1.1.8\bin;%path%
mkdir classes
set CLASSPATH=..\lib\perst11.jar
javac -g -d classes  xml\org\garret\perst\impl\*.java 
cd classes
jar cvf ../../lib/perst11xml.jar .
cd ..
rd /s/q classes
set PATH=%SAVE_PATH%
set CLASSPATH=%SAVE_CLASSPATH%