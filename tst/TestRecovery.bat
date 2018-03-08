del /f testrecovery.dbs
:Repeat
java -classpath .;..\lib\perst.jar TestRecovery
goto Repeat
