move org\garret\perst\impl\OSFile.java .
copy ..\cdc\OSFile.java org\garret\perst\impl
del /f org\garret\perst\impl\OSFile.class
javac -source 1.4 -target 1.4 -g org/garret/perst/*.java org/garret/perst/fulltext/*.java org/garret/perst/impl/*.java 
jar cvf ../lib/perst-cdc.jar org/garret/perst/*.class org/garret/perst/fulltext/*.class org/garret/perst/impl/*.class
move /y OSFile.java org\garret\perst\impl
