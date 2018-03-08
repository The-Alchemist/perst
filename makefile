all: library documentation tests

library: lib/perst.jar lib/perst14.jar

lib/perst14.jar: src14/org/garret/perst/*.java src14/org/garret/perst/impl/*.java src14/org/garret/perst/fulltext/*.java src14/org/garret/perst/impl/sun14/*.java  
	cd src14; javac -source 1.4 -target 1.4 -g org/garret/perst/*.java org/garret/perst/fulltext/*.java org/garret/perst/impl/*.java org/garret/perst/impl/sun14/*.java
	cd src14; jar cvf ../lib/perst14.jar org/garret/perst/*.class org/garret/perst/fulltext/*.class org/garret/perst/impl/*.class org/garret/perst/impl/sun14/*.class

lib/perst.jar: src/org/garret/perst/*.java src/org/garret/perst/fulltext/*.java src/org/garret/perst/impl/*.java src/org/garret/perst/impl/sun14/*.java  
	cd src; javac -source 1.6 -target 1.6 -g org/garret/perst/*.java org/garret/perst/fulltext/*.java org/garret/perst/impl/*.java org/garret/perst/impl/sun14/*.java
	cd src; jar cvf ../lib/perst.jar org/garret/perst/*.class org/garret/perst/fulltext/*.class org/garret/perst/impl/*.class org/garret/perst/impl/sun14/*.class

documentation:  src14/org/garret/perst/*.java src14/org/garret/perst/fulltext/*.java src/org/garret/perst/*.java src/org/garret/perst/fulltext/*.java
	cd src14; javadoc -d ../doc14 -nodeprecated -nosince -public org/garret/perst/*.java org/garret/perst/fulltext/*.java
	cd src; javadoc -source 1.6 -d ../doc -nodeprecated -nosince -public org/garret/perst/*.java org/garret/perst/fulltext/*.java

tests: tst/*.java tst14/*.java
	make -C tst build
	make -C tst14 build

runtests:
	make -C tst runtests
	make -C tst14 runtests

clean:
	find . -name \*.class -exec rm {} \; 
	find . -name \*.dbs -exec rm {} \; 
	find . -name \*.dbz* -exec rm {} \; 
	rm -f tst/*.xml
	rm -f tst14/*.xml

tgz: clean
	chmod +x tst/*.sh
	chmod +x tst14/*.sh
	chmod +x lucene/*.sh
	cd ..; tar cvzf perst.tgz --exclude ".svn" perst

zip: clean
	chmod +x tst/*.sh
	chmod +x tst14/*.sh
	chmod +x lucene/*.sh
	cd ..; zip -r perst.zip perst -x \*/.svn/\*
