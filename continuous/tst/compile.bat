set SAVE_PATH=%PATH%
set PATH=\Program Files\Java\jdk1.5.0_06\bin;%JAVA_HOME%\bin;%path%
javac -source 1.5  -Xlint:unchecked -classpath ../../lib/perst.jar;../../lib/continuous.jar;. -g *.java
set PATH=%SAVE_PATH%