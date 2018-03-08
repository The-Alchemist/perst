del src14\org\garret\perst\*.class
del src14\org\garret\perst\impl\*.class
del src14\org\garret\perst\fulltext\*.class
del src14\org\garret\perst\impl\sun14\*.class
del src14\org\garret\perst\aspectj\*.class
del src\org\garret\perst\aspectj\*.class
del src14\org\garret\perst\jassist\*.class
del src\org\garret\perst\jassist\*.class
del src\org\garret\perst\*.class
del src\org\garret\perst\fulltext\*.class
del src\org\garret\perst\impl\*.class
del tst\*.class
del tst\*.dbs
del tst\*.dbz*
del tst\*.map
del tst\*.xml
del tst\*.log
del src11\org\garret\perst\*.class
del src11\org\garret\perst\impl\*.class
del tst11\*.class
del tst11\*.dbs
del tst11\*.xml
del tst11\reflect\*.class
del tst11\reflect\*.dbs
del continuous\src\org\garret\perst\continuous\*.class
del continuous\tst\*.class
del continuous\tst\*.dbs
del tst\OO7\*.class
del tst\OO7\*.dbs
del tst14\*.class
del tst14\*.dbs
del tst14\*.dbz*
del tst14\*.xml
del tst14\aspectj\*.dbs
del tst14\aspectj\*.class
del tst\aspectj\*.dbs
del tst\aspectj\*.class
del tst14\jassist\*.dbs
del tst14\jassist\*.class
del tst\jassist\*.dbs
del tst\jassist\*.class
del rdf\samples\*.dbs
del rdf\src\org\garret\rdf\*.class
del rdf\src\org\garret\rdf\xml\*.class
del lib\perst-aspectj.jar
del lib\perst14-aspectj.jar
del lib\perst-jassist.jar
del lib\perst14-jassist.jar
del lucene\*.dbs
del lucene\*.class
del blackberry_jde\~*.*
del blackberry_jde\*.rapc
del blackberry_jde\*.cso
del blackberry_jde\*.jad
del blackberry_jde\*.jar
del blackberry_jde\*.debug
rd /s/q blackberry_jde\output
rd /s/q lucene\index
rd /s/q classes
rd /s/q junit_tests\bin
rd /s/q continuous\tst\index
rd /s/q continuous\classes
cd ..
del perst.zip
zip -r perst.zip perst -x "perst/doc/sources/*"