#!/bin/sh

java -Xmx256M -classpath .:../lib/perst14.jar TestMap $1 $2 $3
