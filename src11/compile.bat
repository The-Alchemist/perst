if not defined WTK_HOME set WTK_HOME=c:\WTK2.5.2
set J2MEAPI_PATH=%WTK_HOME%\lib\cldcapi11.jar;%WTK_HOME%\lib\midpapi20.jar;%WTK_HOME%\lib\cldc_1.1.jar;%WTK_HOME%\lib\midp_2.0.jar
set SAVE_PATH=%PATH%
set PATH=c:\jdk1.1.8\bin;classes;%path%
mkdir classes
javac -g -d classes  org\garret\perst\*.java org\garret\perst\impl\*.java cdc\org\garret\perst\impl\*.java xml\org\garret\perst\impl\*.java 
set PATH=%SAVE_PATH%
javac -target 1.2 -source 1.2 -sourcepath weak -classpath classes -g -d classes weak\org\garret\perst\impl\*.java 
javac -target 1.2 -source 1.2 -g -d classes -bootclasspath %J2MEAPI_PATH% -classpath classes -sourcepath weak;reflect;fulltext fulltext\org\garret\perst\fulltext\*.java fulltext\org\garret\perst\impl\*.java reflect\org\garret\perst\*.java reflect\org\garret\perst\impl\*.java reflect\org\garret\perst\reflect\*.java 
cd classes 
jar cvf ../../lib/perst11.jar .
cd ..
rd /s/q classes
