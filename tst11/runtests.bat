del *.dbs
call TestIndex
call TestCompoundIndex
call TestMap
call TestXML
del *.dbs
call TestAltXML
del *.dbs
call TestAltXML 100
call TestRollback
cd reflect
del *.dbs
call JSQLTest
cd ..