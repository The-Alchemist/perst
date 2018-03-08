javac -source 1.5 -g -classpath .;../lib/javassist.jar org/garret/perst/*.java org/garret/perst/fulltext/*.java org/garret/perst/impl/*.java org/garret/perst/impl/sun14/*.java org/garret/perst/jassist/*.java
jar cvf ../lib/perst-jassist.jar org/garret/perst/*.class org/garret/perst/fulltext/*.class org/garret/perst/impl/*.class org/garret/perst/impl/sun14/*.class org/garret/perst/jassist/*.class
