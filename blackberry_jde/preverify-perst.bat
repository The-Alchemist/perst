if not defined BLACKBERY_IDE_HOME set BLACKBERY_IDE_HOME=C:\Program Files\Research In Motion\BlackBerry JDE 4.3.0
"%BLACKBERY_IDE_HOME%\bin\preverify" -classpath "%BLACKBERY_IDE_HOME%\lib\net_rim_api.jar" ..\lib\perst-rms.jar
"%BLACKBERY_IDE_HOME%\bin\preverify" -classpath "%BLACKBERY_IDE_HOME%\lib\net_rim_api.jar" ..\lib\perst-jsr75-reflect.jar
