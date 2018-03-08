del /f testrecovery.dbs
:Repeat
java -classpath .;..\lib\perst14.jar TestRecovery
goto Repeat
