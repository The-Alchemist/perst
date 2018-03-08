del *.dbs
call Simple
del *.dbs
call TestList
del *.dbs
call TestIndex
del *.dbs
call TestIndex altbtree
del *.dbs
call TestIndex altbtree serializable
del *.dbs
call TestIndex inmemory
del *.dbs
call TestIndex2
call TestRndIndex
call TestMap
del *.dbs
call TestMap populate
call TestMap
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
call TestTimeSeries
call TestBit
call TestThickIndex
call TestSet
call TestJSQL
call TestVersion
call TestFullTextIndex
call TestFullTextIndex
call TestFullTextIndex reload
start TestReplic master
call TestReplic slave
