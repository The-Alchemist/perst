#!/bin/sh

rm -f *.dbs
./Simple.sh
rm -f *.dbs
./TestList.sh
rm -f *.dbs
./TestPerf.sh 
rm -f *.dbs
./TestPerf.sh inmemory
rm -f *.dbs
./TestPatricia.sh 
rm -f *.dbs
./TestRegex.sh
rm -f *.dbs *.app *.res
./TestIndex.sh
rm -f *.dbs
./TestIndex.sh altbtree
rm -f *.dbs
./TestIndex.sh inmemory
rm -f *.dbs
./TestIndex.sh map
rm -f *.dbz*
./TestIndex.sh zip
rm -f *.dbs
./TestIndex.sh multifile
rm -f *.dbs
./TestIndex.sh gc
rm -f *.dbs
./TestIndex2.sh
./TestRndIndex.sh
./TestMap.sh
rm -f *.dbs
./TestMap.sh populate
./TestMap.sh
rm -f *.dbs
./TestMap.sh 100
rm -f *.dbs
./TestMap.sh 100 populate
./TestMap.sh 100
./TestCompoundIndex.sh
rm -f *.dbs
./TestCompoundIndex.sh altbtree
./TestMod.sh
rm -f *.dbs
./TestMod.sh pinned
./TestIndexIterator.sh
rm -f *.dbs
./TestIndexIterator.sh altbtree
./TestRtree.sh
./TestR2.sh
./TestTtree.sh
./TestKDTree.sh
./TestKDTree2.sh
rm -f *.dbs
./TestKDTree.sh populate
./TestKDTree.sh populate
rm -f *.dbs
./TestKDTree2.sh populate
./TestKDTree2.sh populate
rm -f *.dbs
./TestRaw.sh
./TestRaw.sh
./TestGC.sh
rm -f *.dbs
./TestGC.sh background
rm -f *.dbs
./TestGC.sh altbtree background
rm -f *.dbs
./TestConcur.sh
./TestConcur.sh
./TestServer.sh
./TestDbServer.sh
./TestXML.sh
rm -f *.dbs
./TestXML.sh altbtree
./TestBackup.sh
./TestBlob.sh
./TestBlob.sh
./CompressDatabase.sh testblob.dbs
rm testblob.dbs
./TestBlob.sh zip
rm testblob.dbz*
./TestRandomBlob.sh
./TestRandomBlob.sh
rm -f *.dbs
./TestAlloc.sh
./TestAlloc.sh
./TestAlloc.sh
./TestLeak.sh
./TestTimeSeries.sh
./TestBit.sh
./TestBitmap.sh
./TestThickIndex.sh
./TestSet.sh
./TestJSQL.sh
./TestJSQLContains.sh
./TestJsqlJoin.sh
./TestJsqlJoin.sh
./TestCodeGenerator.sh
./TestAutoIndices.sh
./TestVersion.sh
./TestFullTextIndex.sh
./TestFullTextIndex.sh
./TestFullTextIndex.sh reload
./TestReplic.sh master & ./TestReplic.sh slave
./TestDynamicObjects.sh
./TestDynamicObjects.sh populate
./TestDynamicObjects.sh 
./TestDecimal.sh
./TestRollback.sh
./TestLoad.sh