#!/bin/sh

java -Xmx256M -classpath .:../lib/perst14.jar TestXML $1 $2 $3
