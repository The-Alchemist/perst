mkdir classes
javac -d classes util\DBConv.java 
cd classes
jar cvf ../../lib/dbconv.jar .
cd ..
rd /s/q classes
