del *.dbs
call Simple
del *.dbs
call TestList
del *.dbs
call TestPerf
del *.dbs
call TestPerf inmemory
del *.dbs
call TestPatricia
del *.dbs
call TestRegex
del *.dbs
call TestApp
del *.res
del *.app
del *.dbs
call TestIndex
del *.dbs
call TestIndex altbtree
del *.dbs
call TestIndex inmemory
del *.dbs
call TestIndex map
del *.dbz*
call TestIndex zip
del *.dbs
call TestIndex multifile
del *.dbs
call TestIndex gc
del *.dbs
call TestIndex2
call TestRndIndex
call TestMap
del *.dbs
call TestMap populate
call TestMap
del *.dbs
call TestMap 100
del *.dbs
call TestMap 100 populate
call TestMap 100
call TestCompoundIndex
del *.dbs
call TestCompoundIndex altbtree
call TestMod
del *.dbs
call TestMod pinned
call TestIndexIterator
del *.dbs
call TestIndexIterator altbtree
call TestRtree
call TestR2
call TestTtree
call TestKDTree
call TestKDTree2
del *.dbs
call TestKDTree populate
call TestKDTree populate
del *.dbs
call TestKDTree2 populate
call TestKDTree2 populate
del *.dbs
call TestRaw
call TestRaw
call TestGC
del *.dbs
call TestGC background
del *.dbs
call TestGC altbtree background
del *.dbs
call TestConcur
call TestConcur
call TestServer
call TestDbServer
call TestXML
del *.dbs
call TestXML altbtree
call TestBackup
call TestBlob
call TestBlob
call CompressDatabase testblob.dbs
del testblob.dbs
call TestBlob zip
del testblob.dbz*
call TestRandomBlob
call TestRandomBlob
del testrndblob.dbs
call TestAlloc
call TestAlloc
call TestAlloc
call TestLeak
call TestTimeSeries
call TestBit
call TestThickIndex
call TestSet
call TestJSQL
call TestJSQLContains
call TestJsqlJoin
call TestJsqlJoin
call TestCodeGenerator
call TestAutoIndices
call TestVersion
call TestFullTextIndex
call TestFullTextIndex
call TestFullTextIndex reload
start TestReplic master
call TestReplic slave
call TestDynamicObjects
call TestDynamicObjects populate
call TestDynamicObjects
call TestDecimal
call TestRollback 
call TestLoad