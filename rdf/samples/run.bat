del *.dbs
java -classpath ..\lib\perstrdf.jar;..\..\lib\perst.jar org.garret.rdf.xml.XmlServer testdb.dbs store-book.xml find-book.xml
