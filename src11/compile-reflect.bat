comset SAVE_PATH=%PATH%
set PATH=c:\jdk1.1.8\bin;%path%
mkdir classes
javac -g -d classes  org\garret\perst\*.java org\garret\perst\impl\*.java cdc\org\garret\perst\impl\*.java xml\org\garret\perst\impl\*.java reflect\org\garret\perst\impl\*.java 
cd classes
jar cvf ../../lib/perst-reflect.jar .
cd ..
