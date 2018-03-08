mkdir classes
javac -d classes util\SerGen.java 
cd classes
jar cvf ../../lib/sergen.jar .
cd ..
rd /s/q classes
